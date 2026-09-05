"""RoadWatch YOLOv8 inference worker.

Consumes versioned frame events from Kafka, runs the deployed YOLOv8s checkpoint,
emits deterministic road-damage events, and routes failed inputs to a DLQ. Kafka
offsets are committed only after all outputs for a frame are acknowledged.
"""
from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime, timezone

import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from ultralytics import YOLO

from contracts import CLASS_NAMES, EVENT_VERSION, severity_from_area

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
MODEL_WEIGHTS = os.getenv("MODEL_WEIGHTS", "./model/weights/best.pt")
CONF_THRES = float(os.getenv("CONF_THRES", "0.25"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))

RAW_TOPIC = "raw_frames"
DET_TOPIC = "road_damage_events"
DLQ_TOPIC = "dead_letter"

FRAMES_CONSUMED = Counter("frames_consumed_total", "Frames processed by YOLO inference")
DETECTIONS = Counter("detections_total", "Detection events emitted", ["damage_type"])
FAILED = Counter("failed_frames_total", "Frames that failed inference processing")
DLQ = Counter("dlq_total", "Messages routed to the dead-letter topic")
INFER_LATENCY = Histogram(
    "inference_latency_seconds",
    "End-to-end YOLO inference latency per frame",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0, 2.0),
)
CONSUMER_UP = Gauge("yolo_consumer_up", "1 if the YOLO inference worker is running")


def main() -> None:
    start_http_server(METRICS_PORT)
    CONSUMER_UP.set(1)

    print(f"[yolo] loading checkpoint: {MODEL_WEIGHTS}")
    model = YOLO(MODEL_WEIGHTS)

    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="roadwatch-inference",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        acks="all",
        retries=5,
        enable_idempotence=True,
    )

    print(f"[yolo] {RAW_TOPIC} -> {DET_TOPIC} (confidence >= {CONF_THRES})")

    for message in consumer:
        frame = message.value
        started = time.perf_counter()
        try:
            if frame.get("event_version") != EVENT_VERSION:
                raise ValueError(f"unsupported frame event_version={frame.get('event_version')!r}")

            img_bytes = base64.b64decode(frame["image_b64"])
            image = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
            if image is None:
                raise ValueError("could not decode frame image")

            height, width = image.shape[:2]
            result = model.predict(image, conf=CONF_THRES, verbose=False)[0]
            latency_ms = (time.perf_counter() - started) * 1000.0

            for det_idx, box in enumerate(result.boxes):
                class_id = int(box.cls[0])
                confidence = float(box.conf[0])
                x1, y1, x2, y2 = [float(value) for value in box.xyxy[0]]
                area_ratio = ((x2 - x1) * (y2 - y1)) / float(width * height)
                damage_type = CLASS_NAMES[class_id] if class_id < len(CLASS_NAMES) else str(class_id)
                event_id = f"evt_{frame['frame_id']}_{det_idx:03d}"

                event = {
                    "event_version": EVENT_VERSION,
                    "event_id": event_id,
                    "trace_id": frame.get("trace_id", frame["frame_id"]),
                    "frame_id": frame["frame_id"],
                    "camera_id": frame.get("camera_id", "dashcam_001"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source_image": frame.get("source_image", ""),
                    "road_segment": frame.get("road_segment", "segment_00"),
                    "damage_type": damage_type,
                    "confidence": round(confidence, 4),
                    "bbox": [x1, y1, x2, y2],
                    "bbox_area_ratio": round(area_ratio, 6),
                    "severity": severity_from_area(area_ratio, confidence),
                    "latency_ms": round(latency_ms, 2),
                }
                producer.send(DET_TOPIC, key=event_id, value=event).get(timeout=30)
                DETECTIONS.labels(damage_type=damage_type).inc()

            INFER_LATENCY.observe(time.perf_counter() - started)
            FRAMES_CONSUMED.inc()
            # The input becomes acknowledged only after all derived events are durable in Kafka.
            consumer.commit()

        except Exception as exc:
            FAILED.inc()
            dlq_record = {
                "event_version": EVENT_VERSION,
                "stage": "yolo_inference",
                "source_topic": RAW_TOPIC,
                "source_id": frame.get("frame_id", "unknown"),
                "reason": str(exc),
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
            producer.send(DLQ_TOPIC, key=dlq_record["source_id"], value=dlq_record).get(timeout=30)
            DLQ.inc()
            # Poison input is acknowledged only after its DLQ record is acknowledged by Kafka.
            consumer.commit()
            print(f"[yolo][ERROR] {frame.get('frame_id', 'unknown')}: {exc}")


if __name__ == "__main__":
    main()
