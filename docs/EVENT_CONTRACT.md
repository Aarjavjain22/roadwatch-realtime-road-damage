# Event Contracts

RoadWatch uses explicit, versioned Kafka contracts so ingestion, YOLO inference, persistence and stream-processing services evolve around stable boundaries.

## `raw_frames`

**Kafka key:** `frame_id`

```json
{
  "event_version": "1.0",
  "frame_id": "frame_000104",
  "trace_id": "frame_000104",
  "camera_id": "dashcam_001",
  "timestamp": "2026-09-04T20:15:30.992Z",
  "source_image": "Japan_000474.jpg",
  "road_segment": "segment_09",
  "image_b64": "..."
}
```

`frame_id` is used as the Kafka key so frame identity remains stable and consistently partitionable.

## `road_damage_events`

**Kafka key:** `event_id`

```json
{
  "event_version": "1.0",
  "event_id": "evt_frame_000104_000",
  "trace_id": "frame_000104",
  "frame_id": "frame_000104",
  "camera_id": "dashcam_001",
  "timestamp": "2026-09-04T20:15:31.110Z",
  "source_image": "Japan_000474.jpg",
  "road_segment": "segment_09",
  "damage_type": "Pothole",
  "confidence": 0.87,
  "bbox": [210.0, 295.0, 402.0, 465.0],
  "bbox_area_ratio": 0.063,
  "severity": "High",
  "latency_ms": 173.4
}
```

## Required detection fields

- `event_version`
- `event_id`
- `frame_id`
- `trace_id`
- `damage_type`
- `confidence`
- `road_segment`

## Validation rules

- `event_version` must equal `1.0`.
- required identifiers must be non-empty.
- `confidence` must be numeric and within `[0, 1]`.
- `damage_type` must be one of `Longitudinal`, `Transverse`, `Alligator`, `Pothole`.
- invalid events are quarantined to `dead_letter` rather than written to operational storage.

## Event identity

Detection IDs are deterministic:

```text
evt_<frame_id>_<detection_index>
```

If a frame is replayed, the same logical detection index regenerates the same event identity. PostgreSQL then uses that identity as its primary-key duplicate guard.

## Traceability

`trace_id` is carried from frame ingestion into every derived detection event so event-level troubleshooting can follow one source frame across Kafka, inference and persistence.

## `dead_letter`

Validation example:

```json
{
  "event_version": "1.0",
  "stage": "analytics_validation",
  "source_topic": "road_damage_events",
  "source_id": "evt_frame_000104_000",
  "reason": "confidence_out_of_range",
  "failed_at": "2026-09-04T20:15:33.410Z",
  "payload": {"...": "original event"}
}
```

Inference example:

```json
{
  "event_version": "1.0",
  "stage": "yolo_inference",
  "source_topic": "raw_frames",
  "source_id": "frame_000104",
  "reason": "could not decode frame image",
  "failed_at": "2026-09-04T20:15:33.410Z"
}
```

The DLQ keeps source identity, stage and failure reason so bad records remain diagnosable and can be replayed intentionally after correction.
