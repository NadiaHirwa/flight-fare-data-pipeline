-- =============================================================================
-- flight_fare_quotes  (PostgreSQL)
-- =============================================================================
-- The analytics serving layer's fact table.
-- Run against the PostgreSQL analytics connection
-- (database: flight_analytics, schema: public — see .env and postgres-init/).
--
-- Named per ADR-011, not `fact_flight_prices`: Phase 0 confirmed the grain is
-- a flight fare quote/offer, not a booking. There is no booking, customer, or
-- reservation entity anywhere in the source.
--
-- GRAIN: one row = one airline's fare quote for one route, at one departure
-- date/time, in one class. Phase 0 found zero duplicates on
-- (airline, source, destination, departure_datetime) across all 57,000 rows,
-- even before including fare_class.
--
-- No dimension tables (ADR-004). A single static extract with no update
-- cadence does not justify surrogate keys and joins; the columns below are
-- denormalized on purpose.
--
-- Types are exactly those declared in docs/data_contract.md. All money is
-- NUMERIC(12,2), never FLOAT or DOUBLE PRECISION (ADR-008) — the Total Fare
-- reconciliation check depends on exact decimal arithmetic, and binary
-- floating point would make its 1.00 tolerance meaningless.
-- =============================================================================

CREATE TABLE IF NOT EXISTS flight_fare_quotes (
    -- -------------------------------------------------------------------
    -- Contract-validated columns (docs/data_contract.md).
    -- NOT NULL here mirrors the contract's nullable=false; a null reaching
    -- this table means validation was bypassed, and the load should fail.
    -- -------------------------------------------------------------------
    airline             VARCHAR(50)     NOT NULL,   -- 24 confirmed values; contract rule is non-empty
    source              VARCHAR(3)      NOT NULL,   -- IATA, 8 Bangladesh domestic codes (ADR-010)
    destination         VARCHAR(3)      NOT NULL,   -- IATA, the 8 domestic + 12 international hubs

    -- Scheduled local departure/arrival. WITHOUT TIME ZONE is deliberate: the
    -- source carries no offset, and a scheduled departure is a wall-clock time
    -- at the origin airport. Coercing it to UTC would invent information.
    departure_datetime  TIMESTAMP       NOT NULL,

    fare_class          VARCHAR(20)     NOT NULL,   -- source column "Class"; Business/Economy/First Class
    seasonality         VARCHAR(20)     NOT NULL,   -- Regular / Winter Holidays / Hajj / Eid

    -- Money. NUMERIC(12,2) per ADR-008. Observed max is 558,987.33, so
    -- precision 12 leaves four orders of magnitude of headroom.
    -- Source values are full-precision floats (e.g. 21131.22502141266);
    -- rounding to 2dp happens in the transform, before this table.
    base_fare           NUMERIC(12,2)   NOT NULL,   -- "Base Fare (BDT)"
    tax_surcharge       NUMERIC(12,2)   NOT NULL,   -- "Tax & Surcharge (BDT)"
    total_fare          NUMERIC(12,2)   NOT NULL,   -- "Total Fare (BDT)"

    -- -------------------------------------------------------------------
    -- Carried-through columns: present in the source, no dedicated
    -- validation rule (docs/data_contract.md, closing paragraph). Nullable
    -- because the contract does not require them — Phase 0 found zero nulls,
    -- but enforcing NOT NULL on a column the contract does not validate
    -- would be a constraint with no rule behind it.
    -- -------------------------------------------------------------------
    source_name         VARCHAR(100)    NULL,
    destination_name    VARCHAR(100)    NULL,
    arrival_datetime    TIMESTAMP       NULL,
    duration_hrs        NUMERIC(5,2)    NULL,       -- observed 0.50 to 15.83
    stopovers           VARCHAR(20)     NULL,       -- Direct / 1 Stop / 2 Stops
    aircraft_type       VARCHAR(50)     NULL,
    booking_source      VARCHAR(20)     NULL,       -- Direct Booking / Online Website / Travel Agency
    days_before_departure SMALLINT      NULL,       -- observed 1 to 90

    -- -------------------------------------------------------------------
    -- Idempotency safety net (docs/MASTER_PLAN.md, "Idempotency and
    -- Reproducibility"). Deterministic SHA-256 over the row's business-key
    -- fields, the same digest written to staging.quarantine.
    --
    -- Truncate-and-reload (ADR-001) already prevents duplication today; this
    -- constraint is what keeps that true if the pipeline is ever pointed at
    -- an incrementally-arriving file. A duplicate then surfaces as a conflict
    -- instead of silently accumulating.
    --
    -- This is NOT NULL + UNIQUE, which makes it the table's effective key. No
    -- surrogate id column was added: nothing references this table (no
    -- dimensions per ADR-004, and the KPI tables aggregate rather than join),
    -- so a generated id would be a column that exists for its own sake.
    -- -------------------------------------------------------------------
    source_record_hash  CHAR(64)        NOT NULL,

    CONSTRAINT uq_flight_fare_quotes_source_record_hash
        UNIQUE (source_record_hash),

    -- -------------------------------------------------------------------
    -- Contract backstops. Validation runs in Python before load (ADR-009,
    -- ETL), so these should never fire — that is the point. They make it
    -- structurally impossible for the serving layer to hold a row that
    -- violates the contract, including via a manual INSERT that bypasses
    -- the DAG entirely.
    --
    -- Not enforced here: the 20-code IATA domain (ADR-010) and the airline
    -- value set. Those lists live in docs/data_contract.md and are applied
    -- by the validation layer; copying 20 literals into DDL would create a
    -- second place to update them and guarantee the two drift apart.
    --
    -- Also not enforced: the observed 2025-01-03..2026-03-31 departure range.
    -- The contract's rule is "parseable timestamp" — the range is an
    -- observation about this file, not a constraint on future ones.
    -- -------------------------------------------------------------------
    CONSTRAINT chk_flight_fare_quotes_airline_non_empty
        CHECK (length(trim(airline)) > 0),
    CONSTRAINT chk_flight_fare_quotes_base_fare_non_negative
        CHECK (base_fare >= 0),
    CONSTRAINT chk_flight_fare_quotes_tax_surcharge_non_negative
        CHECK (tax_surcharge >= 0),
    CONSTRAINT chk_flight_fare_quotes_route_distinct
        CHECK (source <> destination),
    CONSTRAINT chk_flight_fare_quotes_fare_class
        CHECK (fare_class IN ('Business', 'Economy', 'First Class')),
    CONSTRAINT chk_flight_fare_quotes_seasonality
        CHECK (seasonality IN ('Regular', 'Winter Holidays', 'Hajj', 'Eid')),
    -- The 4.42% rule (docs/data_profile.md). Tolerance covers 2dp rounding
    -- only; real violations differ by 445 to 93,165 BDT and are quarantined.
    CONSTRAINT chk_flight_fare_quotes_total_fare_reconciles
        CHECK (abs(total_fare - (base_fare + tax_surcharge)) <= 1.00)
);

-- -----------------------------------------------------------------------------
-- Indexes: one per KPI grouping key, and nothing else. Each traces to a query
-- the pipeline actually runs (docs/kpi_definitions.md).
-- -----------------------------------------------------------------------------
-- Average Fare by Airline + Flight Offer Count by Airline
CREATE INDEX IF NOT EXISTS idx_flight_fare_quotes_airline
    ON flight_fare_quotes (airline);

-- Seasonal Fare Variation
CREATE INDEX IF NOT EXISTS idx_flight_fare_quotes_seasonality
    ON flight_fare_quotes (seasonality);

-- Top Routes
CREATE INDEX IF NOT EXISTS idx_flight_fare_quotes_route
    ON flight_fare_quotes (source, destination);

COMMENT ON TABLE flight_fare_quotes IS
    'One row = one flight fare quote/offer (airline x route x departure datetime x class). Not bookings — see ADR-011.';
