-- =============================================================================
-- KPI 2 — Flight Offer Count by Airline  (PostgreSQL)
-- =============================================================================
-- Definition (docs/kpi_definitions.md):
--     COUNT(*) GROUP BY airline
--
-- Renamed from the assignment's "Booking Count by Airline" — ADR-011. The
-- source contains no booking, customer, or reservation entity; each row is a
-- fare quote/offer. Counting quotes and calling them bookings would
-- misrepresent what this number measures.
--
-- Run against the PostgreSQL analytics connection, after
-- transform_and_load_fact has populated flight_fare_quotes.
--
-- Re-runnable by construction: CREATE IF NOT EXISTS + TRUNCATE + INSERT.
-- =============================================================================

CREATE TABLE IF NOT EXISTS kpi_flight_offer_count_by_airline (
    airline             VARCHAR(50) NOT NULL,
    flight_offer_count  BIGINT      NOT NULL,

    CONSTRAINT pk_kpi_flight_offer_count_by_airline PRIMARY KEY (airline),
    -- A group that produced zero rows cannot exist; if it does, the aggregation
    -- is wrong rather than the data being empty.
    CONSTRAINT chk_kpi_flight_offer_count_positive CHECK (flight_offer_count > 0)
);

TRUNCATE TABLE kpi_flight_offer_count_by_airline;

INSERT INTO kpi_flight_offer_count_by_airline (airline, flight_offer_count)
SELECT
    airline,
    COUNT(*) AS flight_offer_count
FROM flight_fare_quotes
GROUP BY airline;

-- Reconciliation (post_load_quality_check, docs/kpi_definitions.md):
--   SUM(flight_offer_count) = total rows in flight_fare_quotes
-- holds because every row has exactly one non-null airline (enforced NOT NULL
-- on the fact table), so the grouping partitions the fact table exactly.
