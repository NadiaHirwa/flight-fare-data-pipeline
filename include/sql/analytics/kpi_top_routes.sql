-- =============================================================================
-- KPI 3 — Top Routes  (PostgreSQL)
-- =============================================================================
-- Definition (docs/kpi_definitions.md):
--     route  = source || '-' || destination
--     metric = COUNT(*) GROUP BY route, ORDER BY metric DESC
--
-- Run against the PostgreSQL analytics connection, after
-- transform_and_load_fact has populated flight_fare_quotes.
--
-- WHY NO LIMIT, in a KPI called "Top Routes":
-- the reconciliation check requires SUM(counts) = total fact rows
-- (docs/kpi_definitions.md), which only holds if every route is present. A
-- LIMIT here would silently break that check and hide exactly the class of bug
-- it exists to catch. "Top" is served by ordering, not by truncation —
-- consumers take the first N via the index below.
--
-- Re-runnable by construction: CREATE IF NOT EXISTS + TRUNCATE + INSERT.
-- =============================================================================

CREATE TABLE IF NOT EXISTS kpi_top_routes (
    -- 'DAC-CGP' — 3 + 1 + 3. Sized at 7 exactly because both halves are
    -- VARCHAR(3) IATA codes on the fact table; anything longer means the fact
    -- table was loaded with something that is not an IATA code.
    route               VARCHAR(7)  NOT NULL,
    flight_offer_count  BIGINT      NOT NULL,

    CONSTRAINT pk_kpi_top_routes PRIMARY KEY (route),
    CONSTRAINT chk_kpi_top_routes_count_positive CHECK (flight_offer_count > 0)
);

TRUNCATE TABLE kpi_top_routes;

INSERT INTO kpi_top_routes (route, flight_offer_count)
SELECT
    source || '-' || destination AS route,
    COUNT(*)                     AS flight_offer_count
FROM flight_fare_quotes
GROUP BY source, destination
-- Physical row order is not a guarantee in PostgreSQL, so this ORDER BY is not
-- load-bearing — it documents the KPI's intent and makes the inserted order
-- deterministic. `route` is the tiebreaker so equal counts do not reorder
-- between runs. Correct "top N" reads still ORDER BY at query time.
ORDER BY flight_offer_count DESC, route;

-- Serves the actual read pattern: "give me the busiest N routes".
CREATE INDEX IF NOT EXISTS idx_kpi_top_routes_count_desc
    ON kpi_top_routes (flight_offer_count DESC);

-- Reconciliation (post_load_quality_check, docs/kpi_definitions.md):
--   SUM(flight_offer_count) = total rows in flight_fare_quotes
-- holds because every row has exactly one (source, destination) pair, both
-- NOT NULL on the fact table.
