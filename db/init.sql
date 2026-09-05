CREATE TABLE IF NOT EXISTS detection_events (
    event_version   TEXT NOT NULL,
    event_id        TEXT PRIMARY KEY,
    trace_id        TEXT NOT NULL,
    frame_id        TEXT NOT NULL,
    camera_id       TEXT,
    event_time      TIMESTAMPTZ NOT NULL,
    source_image    TEXT,
    road_segment    TEXT NOT NULL,
    damage_type     TEXT NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    severity        TEXT,
    latency_ms      DOUBLE PRECISION,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_segment_time ON detection_events (road_segment, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON detection_events (damage_type, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_trace ON detection_events (trace_id);

CREATE TABLE IF NOT EXISTS segment_window_agg (
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    road_segment    TEXT,
    damage_type     TEXT,
    event_count     BIGINT,
    avg_confidence  DOUBLE PRECISION,
    PRIMARY KEY (window_start, road_segment, damage_type)
);
