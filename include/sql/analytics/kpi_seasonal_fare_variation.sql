-- =============================================================================
-- KPI 4 — Seasonal Fare Variation  (PostgreSQL)
-- =============================================================================
-- Definition (docs/kpi_definitions.md):
--     AVG(total_fare) GROUP BY seasonality
--     Peak season = Winter Holidays, Hajj, Eid.  Non-peak = Regular.
--
-- Phase 0 resolved this KPI's open question: the source already provides a
-- Seasonality column with exactly four values, so no peak/non-peak date
-- boundaries had to be invented (docs/data_profile.md). This is a direct
-- grouping on an existing column — simpler and more reliable than deriving
-- date ranges, and it avoids fabricating a season assumption the data does
-- not support.
--
-- Run against the PostgreSQL analytics connection, after
-- transform_and_load_fact has populated flight_fare_quotes.
--
-- Re-runnable by construction: CREATE IF NOT EXISTS + TRUNCATE + INSERT.
-- =============================================================================

CREATE TABLE IF NOT EXISTS kpi_seasonal_fare_variation (
    seasonality     VARCHAR(20)     NOT NULL,
    avg_total_fare  NUMERIC(12,2)   NOT NULL,   -- NUMERIC, never FLOAT (ADR-008)

    -- The peak/non-peak split from docs/kpi_definitions.md, materialized so
    -- "variation" can be read as peak vs. non-peak without re-encoding the
    -- rule in every downstream query.
    is_peak_season  BOOLEAN         NOT NULL,

    CONSTRAINT pk_kpi_seasonal_fare_variation PRIMARY KEY (seasonality)
);

TRUNCATE TABLE kpi_seasonal_fare_variation;

INSERT INTO kpi_seasonal_fare_variation (seasonality, avg_total_fare, is_peak_season)
SELECT
    seasonality,
    ROUND(AVG(total_fare), 2) AS avg_total_fare,
    -- Written as an explicit membership test rather than `seasonality <> 'Regular'`.
    -- Both are equivalent on the four known values, but this one fails safe: an
    -- unrecognized fifth value would be marked non-peak rather than silently
    -- promoted into the peak group.
    seasonality IN ('Winter Holidays', 'Hajj', 'Eid') AS is_peak_season
FROM flight_fare_quotes
GROUP BY seasonality;

-- Expected shape on this dataset (docs/data_profile.md row counts):
--   Regular 44,525 | Winter Holidays 10,930 | Hajj 942 | Eid 603
-- Four rows, one per season. The small Hajj/Eid groups are real, not noise —
-- worth noting when reading the averages, since they rest on far fewer rows.
