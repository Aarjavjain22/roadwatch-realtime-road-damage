# RoadWatch Architecture

## 1. System objective

RoadWatch converts a continuous road-image stream into two durable outputs:

1. **Validated event-level road-damage telemetry** for operational and downstream use.
2. **Windowed segment-level aggregates** for near-real-time analytics.

A YOLOv8s detector provides the inference stage, while Kafka, Spark, PostgreSQL and the observability stack provide the system boundaries around it.

## 2. Architecture principles

The design follows five principles:

- **Decouple ingestion from inference** so source availability does not depend on model-worker availability.
- **Use explicit event contracts** so independent services evolve around stable schemas.
- **Make replay safe** through manual commits, deterministic IDs and idempotent persistence.
- **Separate raw events from aggregates** so detailed telemetry remains queryable while Spark produces analytical summaries.
- **Treat observability as part of the architecture**, not an afterthought.

## 3. Logical architecture

```mermaid
flowchart TB
    subgraph EDGE[Edge / Source]
        A[Road camera or replay frames]
    end

    subgraph INGEST[Ingestion]
        B[Frame producer]
    end

    subgraph KAFKA[Kafka backbone]
        C[(raw_frames)]
        D[(road_damage_events)]
        E[(dead_letter)]
    end

    subgraph INFER[Inference]
        F[YOLOv8s worker]
        W[(best.pt)]
        W --> F
    end

    subgraph QUALITY[Quality + Persistence]
        G[Contract validator / event sink]
        H[(PostgreSQL detection_events)]
    end

    subgraph STREAM[Streaming Analytics]
        I[Spark Structured Streaming]
        J[(PostgreSQL segment_window_agg)]
    end

    subgraph OPS[Operational Plane]
        K[Kafka Exporter]
        L[Prometheus]
        M[Grafana]
        N[Terraform]
        O[Docker Compose]
        P[GitHub Actions]
    end

    A --> B --> C --> F --> D
    F -->|failure| E
    D --> G --> H
    G -->|invalid contract| E
    D --> I --> J
    C -. lag .-> K
    D -. lag .-> K
    B -. metrics .-> L
    F -. metrics .-> L
    G -. metrics .-> L
    K --> L --> M
    N -. provisions .-> M
    O -. lifecycle .-> B
    O -. lifecycle .-> F
    O -. lifecycle .-> G
    P -. validates .-> O
```

## 4. Data plane sequence

```mermaid
sequenceDiagram
    participant P as Frame Producer
    participant K as Kafka
    participant Y as YOLO Worker
    participant V as Validator / Sink
    participant S as Spark
    participant DB as PostgreSQL

    P->>K: raw_frames(frame_id, trace_id, image_b64)
    K->>Y: consume frame from roadwatch-inference group
    Y->>Y: decode + YOLOv8s inference
    Y->>K: road_damage_events(event_id, class, confidence, bbox, severity)
    Note over Y,K: commit raw-frame offset only after output acknowledgement

    par durable event path
        K->>V: consume detection event
        V->>V: version / field / enum / range validation
        alt valid
            V->>DB: INSERT ... ON CONFLICT DO NOTHING
            Note over V,DB: commit detection offset after successful / duplicate-safe DB result
        else invalid
            V->>K: dead_letter(reason + original payload)
            Note over V,K: commit after DLQ acknowledgement
        end
    and streaming analytics path
        K->>S: consume detection event
        S->>S: event-time parse + 20s watermark + 10s window
        S->>DB: segment_window_agg
    end
```

## 5. Kafka topology

| Topic | Partitions | Key | Producer | Consumer group / processor |
|---|---:|---|---|---|
| `raw_frames` | 6 | `frame_id` | `producer.py` | `roadwatch-inference` |
| `road_damage_events` | 6 | `event_id` | `yolo_consumer.py` | `roadwatch-persistence`, Spark |
| `dead_letter` | 3 | source ID | YOLO / validator | operator tooling |

Explicit topic creation keeps topology intentional and makes partitioning visible in source control.

## 6. Component responsibilities

| Component | Responsibility |
|---|---|
| `streaming/producer.py` | Replays image frames, attaches camera/segment/timestamp metadata, base64-encodes payloads, publishes keyed frame events |
| Kafka | Durable asynchronous boundary between ingestion, inference and downstream processing |
| `streaming/yolo_consumer.py` | Loads `best.pt`, decodes frames, runs YOLOv8s inference, enriches detections with severity, emits deterministic events, routes failures to DLQ |
| `streaming/contracts.py` | Defines event version, allowed classes, required fields and validation helpers |
| `streaming/analytics_consumer.py` | Validates event contract, quarantines invalid records and writes valid events idempotently to PostgreSQL |
| `streaming/spark_stream.py` | Computes 10-second event-time aggregates by segment and damage type with watermarking/checkpointing |
| PostgreSQL | Stores individual validated events and Spark aggregate outputs |
| Kafka Exporter | Exposes topic / partition / consumer-group lag metrics |
| Prometheus | Scrapes producer, inference, persistence and Kafka metrics |
| Grafana | Presents operational health, latency, lag, throughput, DQ and failure signals |
| Terraform | Provisions Grafana resources declaratively |
| Docker Compose | Reproduces the multi-service environment and service dependencies |
| GitHub Actions | Runs contract tests, compile checks and configuration validation |

## 7. Event identity and lineage

Every frame gets a stable `frame_id` and `trace_id`. Derived detections use deterministic IDs:

```text
evt_<frame_id>_<detection_index>
```

This gives a straightforward lineage chain:

```text
source image -> frame_id / trace_id -> YOLO detection -> event_id -> PostgreSQL row / Spark aggregate
```

Deterministic identity is especially important when Kafka replays an input after a restart.

## 8. Persistence model

### `detection_events`

Stores the validated event stream. `event_id` is the primary key. Inserts use `ON CONFLICT (event_id) DO NOTHING`, so replaying the same logical detection does not create another record.

Indexes support common operational queries by segment/time, damage type/time and trace ID.

### `segment_window_agg`

Stores Spark's 10-second event-time windows keyed by window start, road segment and damage type.

### Spark checkpoint

A named Docker volume backs the Structured Streaming checkpoint path so query progress and state survive Spark container restarts.

## 9. Operational plane

The operational plane remains independent from business processing:

```mermaid
flowchart LR
    P[Producer metrics] --> PROM[Prometheus]
    Y[YOLO metrics] --> PROM
    V[Validation / sink metrics] --> PROM
    KE[Kafka Exporter] --> PROM
    PROM --> G[Grafana]
    TF[Terraform] -. dashboard as code .-> G
```

This separation means a backlog or failure can still be diagnosed while a processing stage is degraded.

## 10. Failure domains

| Failure | Expected behavior |
|---|---|
| YOLO worker stopped | `raw_frames` continues accumulating; inference-group lag rises; replay resumes from last committed offset |
| Image decode / inference exception | Source frame is written to `dead_letter`, then its offset is acknowledged |
| Malformed detection event | Validator writes event + reason to `dead_letter`; invalid row never reaches operational storage |
| PostgreSQL unavailable | Persistence consumer does not commit the failing event; Kafka retains it for replay |
| Spark restarted | Persistent checkpoint recovers Structured Streaming progress/state |
| Producer retry | Kafka producer idempotence reduces duplicate writes from retry behavior |
| Replayed detection | Deterministic `event_id` + PostgreSQL PK absorbs duplicate persistence side effects |

## 11. Scale-out mapping

| Repository implementation | Larger deployment mapping |
|---|---|
| Kafka KRaft single broker | Replicated multi-broker managed Kafka |
| 6-partition frame/event topics | Partition count sized to camera and inference throughput |
| One CPU YOLO worker | Consumer-group pool of GPU-backed inference workers |
| PostgreSQL container | Managed HA PostgreSQL / operational event store |
| Local Spark | Managed Spark / Kubernetes / Flink |
| Local Grafana/Prometheus | Central observability stack |
| Docker Compose | Kubernetes / ECS / Nomad |
| `.env` secrets | Secrets manager / workload identity |

The important part is that each scale-out change can happen behind the same event contracts rather than forcing a rewrite of the full system.
