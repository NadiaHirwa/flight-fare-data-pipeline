-- =============================================================================
-- staging.raw_flights  (MySQL)
-- =============================================================================
-- Raw landing zone for Flight_Price_Dataset_of_Bangladesh.csv.
-- Run against the MySQL staging connection (database: `staging`, see .env).
--
-- Purpose: land the source file as-is so that validation (Levels 2/3) and
-- quarantine both read from a persisted, traceable copy rather than from an
-- in-memory frame. Per docs/MASTER_PLAN.md ("Pipeline Architecture"), this is
-- the raw landing zone — NOT the validated layer. No second "validated" copy
-- is persisted in MySQL; valid rows pass straight to the transform step.
--
-- WHY EVERY SOURCE COLUMN IS VARCHAR (loose typing, deliberate):
--   A landing zone must accept whatever the file actually contains. If fares
--   were typed DECIMAL here, a malformed value would fail the bulk load or be
--   silently coerced — and the quarantine table (which stores the ORIGINAL
--   record, per docs/MASTER_PLAN.md "Quarantine design") could no longer show
--   what the source really said. Text preserves the source bytes exactly.
--
--   This does not contradict ADR-008 (money as NUMERIC, never FLOAT). ADR-008
--   exists to prevent binary floating-point rounding error in monetary values;
--   VARCHAR is lossless, and the typed NUMERIC(12,2) guarantee is enforced
--   where money is actually computed and served — the PostgreSQL fact table
--   and KPI tables (see include/sql/analytics/).
--
--   Note the source is genuinely full-precision, not 2dp:
--   Base Fare "21131.22502141266" — rounding to NUMERIC(12,2) is a transform
--   step, and it belongs in the transform, not in the landing zone.
--
--   Widths are uniform and generous for the same reason: column width is not a
--   validation mechanism here. Observed maxima are well inside 255 (longest is
--   `Destination Name` at 57 chars).
--
-- Column mapping — identifiers are snake_cased, the column set and order
-- mirror the CSV exactly (all 17 columns, see docs/data_profile.md):
--   Airline                -> airline               Booking Source        -> booking_source
--   Source                 -> source                Base Fare (BDT)       -> base_fare_bdt
--   Source Name            -> source_name           Tax & Surcharge (BDT) -> tax_surcharge_bdt
--   Destination            -> destination           Total Fare (BDT)      -> total_fare_bdt
--   Destination Name       -> destination_name      Seasonality           -> seasonality
--   Departure Date & Time  -> departure_datetime    Days Before Departure -> days_before_departure
--   Arrival Date & Time    -> arrival_datetime
--   Duration (hrs)         -> duration_hrs
--   Stopovers              -> stopovers
--   Aircraft Type          -> aircraft_type
--   Class                  -> fare_class            ("class" is a reserved word in
--                                                    several engines; renamed once,
--                                                    here and in the fact table)
--
-- Idempotency: this file is DDL only. The load task truncates before insert
-- (ADR-001, truncate-and-reload) — truncation is not baked in here so that
-- applying the schema is separable from loading data.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_flights (
    -- ---- the 17 source columns, in file order, all loosely typed ----
    airline                 VARCHAR(255)  NULL,
    source                  VARCHAR(255)  NULL,
    source_name             VARCHAR(255)  NULL,
    destination             VARCHAR(255)  NULL,
    destination_name        VARCHAR(255)  NULL,
    departure_datetime      VARCHAR(255)  NULL,
    arrival_datetime        VARCHAR(255)  NULL,
    duration_hrs            VARCHAR(255)  NULL,
    stopovers               VARCHAR(255)  NULL,
    aircraft_type           VARCHAR(255)  NULL,
    fare_class              VARCHAR(255)  NULL,
    booking_source          VARCHAR(255)  NULL,
    base_fare_bdt           VARCHAR(255)  NULL,
    tax_surcharge_bdt       VARCHAR(255)  NULL,
    total_fare_bdt          VARCHAR(255)  NULL,
    seasonality             VARCHAR(255)  NULL,
    days_before_departure   VARCHAR(255)  NULL,

    -- ---- pipeline metadata (not from the source file) ----
    -- 1-based position in the source CSV, excluding the header. Assigned by the
    -- load task as it reads the file — deliberately NOT an AUTO_INCREMENT: this
    -- records where the row sat in the source, which is a property of the file,
    -- not of the insertion.
    --
    -- This is what staging.quarantine.source_row_number (NOT NULL) is populated
    -- from. Validation reads a row from here, rejects it, and carries its exact
    -- source position across, which is what makes "CSV row 1,753 was rejected,
    -- here is exactly why" a direct lookup (docs/MASTER_PLAN.md, Quarantine
    -- design). It has to be persisted at load time: MySQL guarantees no
    -- insertion order, so the row number cannot be recovered after the fact.
    source_row_number       BIGINT UNSIGNED NOT NULL,

    -- FK-in-spirit to staging.pipeline_runs.pipeline_run_id. Not declared as a
    -- real FOREIGN KEY: the run row is INSERTed with status 'running' before
    -- staging begins, but a hard constraint would make the load fail on a
    -- partially-written audit row rather than surface it as a reconciliation
    -- mismatch, which is the more informative failure (see reconciliation_check).
    pipeline_run_id         VARCHAR(64)   NOT NULL,
    ingested_at             DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    -- Validation reads one run's rows at a time, so pipeline_run_id leads.
    --
    -- UNIQUE because a source row number can occur at most once per run by
    -- construction. That makes a double-load — the same file staged twice under
    -- one run_id without an intervening truncate — fail loudly instead of
    -- silently doubling every count the quality gate (ADR-005) and the
    -- reconciliation equations depend on. Same reasoning as the fact table's
    -- UNIQUE source_record_hash, applied at the landing zone.
    --
    -- MySQL can use a composite index's leftmost prefix, so this one index also
    -- serves the plain "give me every row for this run" scan — no second index
    -- on pipeline_run_id alone is needed.
    UNIQUE KEY uq_raw_flights_run_row (pipeline_run_id, source_row_number)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci
COMMENT = 'Raw landing zone: source CSV as-is, loosely typed. Not the validated layer.';
