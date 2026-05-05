SELECT 'CREATE DATABASE ops_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ops_db')\gexec

\c ops_db

CREATE SCHEMA IF NOT EXISTS elementary;
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
CREATE SCHEMA IF NOT EXISTS ingestion;
CREATE SCHEMA IF NOT EXISTS web;




-- finops for trino---------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS finops;

-- create table for trino_finops_logs
CREATE TABLE IF NOT EXISTS finops.trino_finops_logs (
    query_id VARCHAR(100),
    user_name VARCHAR(100),
    source_app VARCHAR(100),
    catalog_name VARCHAR(100),
    schema_name VARCHAR(100),
    query_state VARCHAR(50),
    created_at TIMESTAMPTZ,
    query_text TEXT,
    cpu_time_s DOUBLE PRECISION,
    failed_cpu_time_s DOUBLE PRECISION,
    wall_time_s DOUBLE PRECISION,
    scanned_bytes BIGINT,
    peak_memory_bytes BIGINT,

    PRIMARY KEY (query_id, created_at)
)PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS idx_finops_user_app
ON finops.trino_finops_logs (user_name, source_app);

-- Partial Index
CREATE INDEX IF NOT EXISTS idx_finops_non_finished_queries
ON finops.trino_finops_logs (query_id)
WHERE query_state != 'FINISHED';

-- Index for CPU time desc
CREATE INDEX IF NOT EXISTS idx_finops_cpu_time_desc
ON finops.trino_finops_logs (cpu_time_s DESC NULLS LAST);




------------Job_ingestion------------------------------------------------------------------------------------------------- (Partition theo start_time)
CREATE TABLE IF NOT EXISTS ingestion.ingestion_kafka_state (
    batch_id VARCHAR(50),
    data_type VARCHAR(50),
    ticker VARCHAR(20),
    created_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Composite Primary Key
    PRIMARY KEY (ticker, data_type)
);

CREATE TABLE IF NOT EXISTS ingestion.ingestion_watermark (
    ticker VARCHAR(20),
    data_type VARCHAR(50),
    ticker_status VARCHAR(20),
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, data_type)
);

-- part_man setup ------------------------------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM partman.part_config WHERE parent_table = 'finops.trino_finops_logs') THEN
        PERFORM partman.create_parent(
            p_parent_table := 'finops.trino_finops_logs',
            p_control := 'created_at',
            p_type := 'range',
            p_interval := '1 day',
            p_premake := 5,
            p_default_table := 'true'
        );

        UPDATE partman.part_config
        SET retention = '60 days',
            retention_keep_table = false,
            retention_keep_index = false,
            infinite_time_partitions = true
        WHERE parent_table = 'finops.trino_finops_logs';

    END IF;

END $$;
