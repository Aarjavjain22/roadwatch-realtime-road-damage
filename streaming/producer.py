"""Replay image frames into Kafka `raw_frames` with stable event identity."""
from __future__ import annotations

import argparse
import base64
import glob
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server

EVENT_VERSION = "1.0"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
FRAMES_DIR = os.getenv("FRAMES_DIR", "./data/frames")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
FRAME_DELAY = float(os.getenv("FRAME_DELAY", "0.15"))
RAW_TOPIC = "raw_frames"

FRAMES_PRODUCED = Counter("frames_produced_total", "Frames published to Kafka")
PRODUCER_UP = Gauge("producer_up", "1 if the producer is running")


def road_segment_for(index: int, segments: int = 12) -> str:
    return f"segment_{(index % segments) + 1:02d}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--max-frames", type=int, default=0)
    args = parser.parse_args()

    start_http_server(METRICS_PORT)
    PRODUCER_UP.set(1)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
        enable_idempotence=True,
    )

    images = sorted(glob.glob(os.path.join(FRAMES_DIR, "*.jpg")) + glob.glob(os.path.join(FRAMES_DIR, "*.png")))
    if not images:
        raise SystemExit(f"No images found in {FRAMES_DIR}")

    print(f"[producer] {len(images)} replay frames -> {RAW_TOPIC}")
    idx = 0
    while True:
        for path in images:
            if args.max_frames and idx >= args.max_frames:
                break
            with open(path, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode("utf-8")
            frame_id = f"frame_{idx:06d}"
            msg = {
                "event_version": EVENT_VERSION,
                "frame_id": frame_id,
                "trace_id": frame_id,
                "camera_id": "dashcam_001",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source_image": os.path.basename(path),
                "road_segment": road_segment_for(idx),
                "image_b64": img_b64,
            }
            producer.send(RAW_TOPIC, key=frame_id, value=msg).get(timeout=30)
            FRAMES_PRODUCED.inc()
            idx += 1
            time.sleep(FRAME_DELAY)
        producer.flush()
        if not args.loop or (args.max_frames and idx >= args.max_frames):
            break


if __name__ == "__main__":
    main()
