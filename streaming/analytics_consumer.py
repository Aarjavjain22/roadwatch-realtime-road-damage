"""Validate RoadWatch events, persist them idempotently, and expose DQ metrics."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server

from contracts import EVENT_VERSION, validation_error

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://roadwatch:roadwatch@localhost:5432/roadwatch")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8003"))
DET_TOPIC = "road_damage_events"
DLQ_TOPIC = "dead_letter"

EVENTS_STORED = Counter("events_stored_total", "Detection events written to Postgres")
SCHEMA_INVALID = Counter("dq_schema_invalid_total", "Events failing schema validation")
DUPLICATES = Counter("dq_duplicate_total", "Duplicate events absorbed")
OUT_OF_RANGE = Counter("dq_out_of_range_total", "Events with confidence outside [0,1]")
DLQ = Counter("analytics_dlq_total", "Invalid events written to dead letter")
ANALYTICS_UP = Gauge("analytics_consumer_up", "1 if analytics consumer is running")


def connect_pg():
    for _ in range(30):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.autocommit = True
            return conn
        except Exception as exc:
            print(f"[analytics] waiting for postgres: {exc}")
            time.sleep(2)
    raise SystemExit("could not connect to postgres")


def main():
    start_http_server(METRICS_PORT)
    ANALYTICS_UP.set(1)
    conn = connect_pg()
    cur = conn.cursor()

    consumer = KafkaConsumer(
        DET_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="roadwatch-persistence",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        enable_idempotence=True,
    )

    for message in consumer:
        e = message.value
        error = validation_error(e)
        if error:
            if error in {"schema_invalid", "unsupported_event_version", "unknown_damage_type"}:
                SCHEMA_INVALID.inc()
            elif error == "confidence_out_of_range":
                OUT_OF_RANGE.inc()
            producer.send(DLQ_TOPIC, value={
                "event_version": EVENT_VERSION,
                "stage": "analytics_validation",
                "source_topic": DET_TOPIC,
                "source_id": e.get("event_id", "unknown"),
                "reason": error,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "payload": e,
            }).get(timeout=30)
            DLQ.inc()
            consumer.commit()
            continue

        try:
            cur.execute(
                """
                INSERT INTO detection_events
                    (event_version, event_id, trace_id, frame_id, camera_id, event_time,
                     source_image, road_segment, damage_type, confidence, severity, latency_ms)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    e["event_version"], e["event_id"], e["trace_id"], e["frame_id"],
                    e.get("camera_id"), e.get("timestamp") or datetime.now(timezone.utc).isoformat(),
                    e.get("source_image"), e["road_segment"], e["damage_type"],
                    e["confidence"], e.get("severity"), e.get("latency_ms"),
                ),
            )
            if cur.rowcount == 0:
                DUPLICATES.inc()
            else:
                EVENTS_STORED.inc()
            consumer.commit()
        except Exception as exc:
            print(f"[analytics][ERROR] {exc}")


if __name__ == "__main__":
    main()
