-- =============================================================================
-- KPI 1 — Average Fare by Airline  (PostgreSQL)
-- =============================================================================
-- Definition (docs/kpi_definitions.md):
--     AVG(total_fare) GROUP BY airline
--
-- Run against the PostgreSQL analytics connection, after
-- transform_and_load_fact has populated flight_fare_quotes.
--
-- This is the ELT-shaped half of the architecture (ADR-009): the fact table is
-- loaded first, then aggregated in SQL. Plain SQL via Airflow's Postgres
-- operator, not a dbt model — ADR-006.
--
-- Re-runnable by construction: CREATE IF NOT EXISTS + TRUNCATE + INSERT means
-- an Airflow retry produces the same end state as a single successful run
-- (docs/MASTER_PLAN.md, "Retries must be safe by construction").
-- =============================================================================

CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    airline         VARCHAR(50)     NOT NULL,
    -- NUMERIC(12,2), never FLOAT — ADR-008 applies to derived money too.
    avg_total_fare  NUMERIC(12,2)   NOT NULL,

    -- One row per airline is the KPI's grain; declaring it as the key means a
    -- duplicated-group bug fails the INSERT instead of producing a table that
    -- looks fine and sums wrong.
    CONSTRAINT pk_kpi_avg_fare_by_airline PRIMARY KEY (airline)
);

TRUNCATE TABLE kpi_avg_fare_by_airline;

INSERT INTO kpi_avg_fare_by_airline (airline, avg_total_fare)
SELECT
    airline,
    -- Rounded explicitly rather than relying on the column's implicit cast, so
    -- the rounding is visible where the value is computed.
    ROUND(AVG(total_fare), 2) AS avg_total_fare
FROM flight_fare_quotes
GROUP BY airline;

-- Reconciliation (post_load_quality_check, docs/kpi_definitions.md):
--   per airline: MIN(total_fare) <= avg_total_fare <= MAX(total_fare)
-- checked against flight_fare_quotes, which catches an aggregation that ran
-- but grouped or averaged the wrong thing.
