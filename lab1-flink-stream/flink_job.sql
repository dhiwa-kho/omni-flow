SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'Omni-Flow Flink 10s Tumbling Window & Fraud Filter';

CREATE TABLE raw_payments (
    tx_id STRING,
    event_ts TIMESTAMP(3),
    merchant STRING,
    category STRING,
    amount DOUBLE,
    country STRING,
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_payments',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_stream_engine_v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE flink_windowed_output (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    category STRING,
    tx_count BIGINT,
    total_amount DOUBLE,
    max_single_tx DOUBLE,
    high_risk_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'flink_windowed_output',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

INSERT INTO flink_windowed_output
SELECT
    TUMBLE_START(event_ts, INTERVAL '10' SECOND) AS window_start,
    TUMBLE_END(event_ts, INTERVAL '10' SECOND) AS window_end,
    category,
    COUNT(*) AS tx_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(MAX(amount), 2) AS max_single_tx,
    SUM(CASE WHEN amount >= 25000 THEN 1 ELSE 0 END) AS high_risk_count
FROM raw_payments
GROUP BY
    TUMBLE(event_ts, INTERVAL '10' SECOND),
    category;

