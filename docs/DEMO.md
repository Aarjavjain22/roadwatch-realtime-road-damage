# RoadWatch Demo

**[▶ Watch the full recorded demo](../demo/RoadWatch_Demo.mp4)**

The demo presents RoadWatch as one connected system rather than separate model and data-engineering pieces.

## What to watch for

1. **Continuous frame ingestion** — the producer publishes image frames into `raw_frames`.
2. **Real YOLO inference** — `yolo-consumer` loads the trained checkpoint and emits structured detections.
3. **Kafka decoupling** — ingestion and inference progress independently through consumer offsets.
4. **Durable event storage** — validated detections are written to PostgreSQL.
5. **Streaming aggregation** — Spark groups detections into 10-second segment/damage windows.
6. **Operational telemetry** — Grafana surfaces throughput, inference latency, failures, DQ counters and Kafka lag.
7. **Backlog recovery** — stopping the YOLO worker allows Kafka lag to grow while the producer continues; restarting the worker replays retained frames from the last committed offset.

## Demo preview

![RoadWatch demo preview](assets/demo-preview.gif)

## Useful follow-up

After watching the video, review:

- [Architecture](ARCHITECTURE.md)
- [Reliability](RELIABILITY.md)
- [Event Contracts](EVENT_CONTRACT.md)
- [Model](MODEL.md)
