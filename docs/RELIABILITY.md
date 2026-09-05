# Reliability, Recovery and Observability

## Reliability objective

RoadWatch is designed so transient service failures create **visible backlog and recoverable replay**, rather than silent loss. Kafka provides the durable work boundary; consumers acknowledge records only after downstream work reaches a safe boundary.

## Delivery semantics

The platform uses **at-least-once delivery with idempotent downstream effects**.

That description is intentionally precise: Kafka, Spark and PostgreSQL do not participate in one distributed transaction, so RoadWatch does not claim universal end-to-end exactly-once processing.

## Implemented controls

| Control | Implementation | Protects against |
|---|---|---|
| Broker acknowledgement | `acks=all` | Producer returning success before Kafka acknowledges the record |
| Producer idempotence | `enable_idempotence=True` | Duplicate Kafka writes caused by producer retry behavior |
| Manual offset commits | `enable_auto_commit=False` | Advancing a consumer before downstream work succeeds |
| Deterministic event identity | frame ID + detection index | Duplicate logical detection identity after replay |
| PostgreSQL primary key | `event_id` | Duplicate durable records |
| Idempotent insert | `ON CONFLICT DO NOTHING` | Replay / retry persistence side effects |
| Dead-letter topic | `dead_letter` | Poison frames and invalid detection events |
| Versioned contracts | `event_version = 1.0` | Silent schema drift between independent stages |
| Range / enum checks | confidence and damage type validation | Malformed records reaching storage |
| Spark checkpointing | persistent `/checkpoint` volume | Lost stream progress/state after Spark restart |
| Event-time watermark | 20 seconds | Unbounded streaming state from late data |
| Consumer-lag metrics | Kafka Exporter | Stalled or under-provisioned consumers |
| Service metrics | Prometheus | Silent throughput / latency / DQ degradation |
| Operational dashboard | Grafana | Fragmented troubleshooting across services |
| Container health checks | producer / YOLO / persistence metrics endpoints | Invisible dead or unhealthy services |

## Detector outage

When `yolo-consumer` is stopped:

1. `producer` continues publishing `raw_frames`.
2. Kafka retains those records.
3. The `roadwatch-inference` consumer-group lag grows.
4. Kafka Exporter exposes that lag to Prometheus.
5. Grafana makes the backlog visible.
6. When the worker restarts, it resumes from the last committed offset.
7. Re-generated detections receive the same deterministic `event_id` for the same frame/detection index.
8. PostgreSQL absorbs duplicate event IDs if a replay overlaps a prior side effect.

## PostgreSQL outage

`analytics-consumer` commits a detection offset only after the insert succeeds or returns an idempotent duplicate result. If PostgreSQL is unavailable, the current Kafka record remains uncommitted and can be replayed after database recovery.

## Invalid event

The persistence consumer validates each detection before storage. If the event fails version, required-field, class or confidence validation:

1. the original payload and reason are wrapped in a DLQ record;
2. the DLQ record is synchronously acknowledged by Kafka;
3. only then is the invalid detection offset committed.

This prevents poison records from blocking the stream while preserving forensic context.

## Inference failure

Frame decode or YOLO inference exceptions follow the same poison-record pattern: the source frame ID, failure stage and reason are sent to `dead_letter` before the input offset is acknowledged.

## Spark restart

Spark Structured Streaming stores its checkpoint in a persistent named volume. On restart it can recover query progress/state. The 20-second event-time watermark bounds retained state for late-arriving events.

## Observable signals

### Throughput

- produced frames / second
- consumed frames / second
- detection events / second
- persisted events / second

### Latency

- inference latency histogram
- p50 / p95 / p99 views

### Backpressure

- Kafka consumer lag by group/topic/partition

### Data quality

- schema-invalid events
- unsupported versions / classes
- confidence values outside `[0,1]`
- duplicate events absorbed
- DLQ volume

### Liveness

- producer up
- YOLO consumer up
- analytics consumer up

## Recovery drill

```bash
# Establish baseline
docker compose ps

# Create an inference backlog
docker compose stop yolo-consumer

# Observe roadwatch-inference lag in Grafana / Prometheus.
# The producer remains active.

# Recover
docker compose start yolo-consumer

# Verify retained frames replay and the backlog falls.
```

The recorded demo shows this behavior end to end: **[RoadWatch_Demo.mp4](../demo/RoadWatch_Demo.mp4)**.
