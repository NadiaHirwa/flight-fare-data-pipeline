-- =============================================================================
-- staging.pipeline_runs  (MySQL)
-- =============================================================================
-- Audit / run metadata, populated by the DAG itself.
-- Run against the MySQL staging connection (database: `staging`, see .env).
--
-- Column set matches docs/MASTER_PLAN.md "Pipeline Audit / Run Metadata"
-- exactly. Its purpose is to make reconciliation a query, not a guess:
--
--   source_row_count = valid_row_count + rejected_row_count   (must hold)
--   valid_row_count  = loaded_row_count                        (must hold,
--                                                    given truncate-and-reload)
--
-- If either equation fails on a run, that is a detectable data-loss signal —
-- checked by the reconciliation_check task.
--
-- NOT truncated between runs. This is the one table in the staging layer that
-- accumulates: truncate-and-reload (ADR-001) applies to the data tables, and
-- an audit log that gets wiped every run cannot audit anything.
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    -- Airflow's run_id (e.g. 'manual__2026-08-14T09:15:00.123456+00:00'),
    -- passed through XCom as the one identifier every task shares. Not a
    -- generated UUID: reusing Airflow's own run_id means a row here maps
    -- directly to a run in the UI with no extra lookup table.
    pipeline_run_id         VARCHAR(64)     NOT NULL,

    source_file             VARCHAR(512)    NOT NULL,

    -- SHA-256 of the source file's bytes. Detects "the exact same file was
    -- submitted twice" for free, given this table already exists
    -- (docs/MASTER_PLAN.md, "File-level protection").
    source_file_checksum    CHAR(64)        NOT NULL,

    -- All five counts are NULL until the task that establishes them completes,
    -- which is what makes a partially-failed run legible after the fact: the
    -- last non-NULL count tells you how far the pipeline actually got.
    --   source_row_count   set by validate_source_schema (rows in the CSV)
    --   staged_row_count   set by load_to_mysql_staging  (rows in raw_flights)
    --   valid/rejected     set by validate_and_quarantine
    --   loaded_row_count   set by transform_and_load_fact
    source_row_count        BIGINT UNSIGNED NULL,
    staged_row_count        BIGINT UNSIGNED NULL,
    valid_row_count         BIGINT UNSIGNED NULL,
    rejected_row_count      BIGINT UNSIGNED NULL,
    loaded_row_count        BIGINT UNSIGNED NULL,

    started_at              DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    -- NULL while the run is in flight; set exactly once when it reaches a
    -- terminal status.
    completed_at            DATETIME(6)     NULL,

    -- 'quality_gate_failed' is deliberately distinct from 'failed': the gate
    -- tripping means the data was abnormal (rejection rate >= 6%, ADR-005),
    -- not that the pipeline malfunctioned. Conflating them would lose exactly
    -- the distinction the gate exists to draw.
    status                  VARCHAR(20)     NOT NULL DEFAULT 'running',

    CONSTRAINT pk_pipeline_runs PRIMARY KEY (pipeline_run_id),
    CONSTRAINT chk_pipeline_runs_status
        CHECK (status IN ('running', 'success', 'failed', 'quality_gate_failed')),

    -- "most recent runs" / "has this file been ingested before"
    KEY idx_pipeline_runs_started_at (started_at),
    KEY idx_pipeline_runs_source_file_checksum (source_file_checksum)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci
COMMENT = 'Per-run audit metadata. Accumulates across runs; never truncated.';
