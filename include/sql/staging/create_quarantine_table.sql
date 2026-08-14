-- =============================================================================
-- staging.quarantine  (MySQL)
-- =============================================================================
-- Rejected rows, never silently dropped (ADR-003).
-- Run against the MySQL staging connection (database: `staging`, see .env).
--
-- Column set matches docs/MASTER_PLAN.md "Quarantine design" exactly — the
-- eight columns listed there, in that order, and nothing else. No surrogate
-- key was added: a row can legitimately fail more than one rule, so
-- (pipeline_run_id, source_row_number) is NOT unique and cannot serve as a
-- primary key, and an AUTO_INCREMENT id would be a column nothing references.
-- InnoDB's hidden clustered index covers the storage side either way.
--
-- One row here = one (rejected row x violated rule) pair.
-- =============================================================================

CREATE TABLE IF NOT EXISTS quarantine (
    -- The original values as they appeared in the source, keyed by column name.
    -- JSON rather than a flattened copy of raw_flights: the point of quarantine
    -- is "here is exactly what the file said", and JSON keeps that intact and
    -- queryable without coupling this table to the source's column list.
    original_record     JSON            NOT NULL,

    -- 1-based row number within the source CSV, excluding the header — so
    -- "CSV row 1,753 was rejected, here is exactly why" is a direct lookup
    -- rather than a manual search (docs/MASTER_PLAN.md, Quarantine design).
    source_row_number   BIGINT UNSIGNED NOT NULL,

    -- SHA-256 hex digest of the row's business-key fields. Deliberately the
    -- SAME hash the fact table carries for idempotency — computed once, reused
    -- here at near-zero cost, and it makes "did this rejected row ever get
    -- loaded?" a single join against analytics.flight_fare_quotes.
    source_record_hash  CHAR(64)        NOT NULL,

    -- Human-readable explanation, e.g.
    -- 'Total Fare 26300.91 does not reconcile with Base 21131.23 + Tax 5169.68 (diff 0.00 > 1.00 tolerance)'.
    rejection_reason    TEXT            NOT NULL,

    -- 1 = file-level schema, 2 = data quality, 3 = business rule (ADR-003).
    -- In practice only 2 and 3 appear: Level 1 failures are structural, stop
    -- the pipeline fail-fast, and are not row-level events. 1 is permitted in
    -- the domain anyway so the level vocabulary stays consistent with ADR-003.
    validation_level    TINYINT UNSIGNED NOT NULL,

    -- Stable machine-readable rule identifier, e.g. 'total_fare_reconciliation',
    -- 'destination_not_in_iata_domain', 'source_equals_destination'.
    -- Distinct from rejection_reason, which is per-row prose: this groups.
    rule_violated       VARCHAR(100)    NOT NULL,

    pipeline_run_id     VARCHAR(64)     NOT NULL,
    ingested_at         DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT chk_quarantine_validation_level CHECK (validation_level IN (1, 2, 3)),

    -- Access paths that actually exist:
    --   quality gate + reconciliation count rejects for one run
    KEY idx_quarantine_pipeline_run_id (pipeline_run_id),
    --   "why was CSV row N rejected on this run" -- direct lookup
    KEY idx_quarantine_run_row (pipeline_run_id, source_row_number),
    --   "which rule is doing the rejecting" -- expected to be dominated by
    --   total_fare_reconciliation at ~4.42% (docs/data_profile.md)
    KEY idx_quarantine_rule_violated (rule_violated)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci
COMMENT = 'Rejected rows with full traceability back to their source position. One row per (rejected row x violated rule).';
