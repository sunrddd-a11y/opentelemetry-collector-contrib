-- SkyWalking Cola profile task table (standalone MergeTree).
-- Applied automatically when clickhouse.create_schema is true (the default).
-- Snapshots, otel_agents, Distributed wrappers and otel_agent_mv are created
-- by the clickhouse_cola exporter.
--
-- First-create only. If old objects exist, drop them before startup:
--   DROP TABLE IF EXISTS otel.sw_profile_flame_edges_mv;
--   DROP TABLE IF EXISTS otel.sw_profile_flame_edges;
--   DROP TABLE IF EXISTS otel.sw_profile_task_finish;
--   DROP TABLE IF EXISTS otel.sw_profile_tasks;

CREATE TABLE IF NOT EXISTS otel.sw_profile_tasks
(
    task_id                   String,
    service                   String,
    service_instance          String DEFAULT '',
    endpoint_name             String,
    duration_seconds          UInt32,
    min_duration_threshold_ms UInt32,
    dump_period_ms            UInt32,
    max_sampling_count        UInt32,
    start_time                DateTime64(3),
    create_time               DateTime64(3),
    serial_number             String,
    enabled                   UInt8 DEFAULT 1,
    status                    LowCardinality(String) DEFAULT 'running',
    extra                     String DEFAULT '',
    `delete`                  UInt8 DEFAULT 0,
    tts                       DateTime DEFAULT now() CODEC(DoubleDelta, LZ4)
)
ENGINE = ReplacingMergeTree
ORDER BY task_id
SETTINGS index_granularity = 8192;
