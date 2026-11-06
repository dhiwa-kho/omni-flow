CREATE DATABASE IF NOT EXISTS omni_olap;

CREATE TABLE IF NOT EXISTS omni_olap.sensor_events (
    event_id String,
    seq_no UInt64,
    event_time DateTime64(3),
    zone LowCardinality(String),
    device_id LowCardinality(String),
    temperature Float64,
    energy_kwh Float64,
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (event_id);

