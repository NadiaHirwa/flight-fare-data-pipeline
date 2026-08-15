"""KPI-level sanity checks against the loaded analytics layer.

Implements the post_load_quality_check task body (ADR-007). PostgreSQL only —
everything it inspects lives in the analytics database.

These are the checks from docs/kpi_definitions.md:

    SUM(flight_offer_count_by_airline) = total valid fact rows
    SUM(top_routes counts)             = total valid fact rows
    per airline: MIN(total_fare) <= AVG(total_fare) <= MAX(total_fare)

Their purpose is stated there too: they "catch a KPI table that exists and is
non-null but is logically wrong — a different failure class than a missing
table or a null value". A KPI table can be present, fully populated, and still
be built on the wrong GROUP BY; only comparing it back against the fact table
detects that.

The comparison logic is a pure function over a snapshot (evaluate_post_load),
separate from the SQL that gathers it. That is what lets every failure mode be
tested with a hand-built fixture instead of a broken database.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..shared.connections import get_analytics_engine

logger = logging.getLogger(__name__)

# The analytics tables this check reads. Stated here rather than imported from
# src/transformation/ and src/kpi/ so that a downstream checker does not depend
# on the stages it audits; tests pin these to the authoritative definitions so
# they cannot drift silently.
FACT_TABLE = "flight_fare_quotes"
KPI_AVG_FARE_TABLE = "kpi_avg_fare_by_airline"
KPI_OFFER_COUNT_TABLE = "kpi_flight_offer_count_by_airline"
KPI_TOP_ROUTES_TABLE = "kpi_top_routes"


class QualityCheckError(Exception):
    """A post-load sanity check failed.

    Raised with every failing check described in one message, rather than
    stopping at the first: a run that broke two KPIs should say so once, not
    require two more runs to discover the rest.
    """


@dataclass(frozen=True)
class AirlineFareBounds:
    """One airline's fare bounds from the fact table, next to its KPI average.

    Any field may be None, and which one is None is itself the finding:
      average None -> the airline has fact rows but no row in the KPI table
      minimum/maximum None -> the KPI table names an airline the fact table
                              does not contain
    """

    airline: str
    minimum: Decimal | None
    average: Decimal | None
    maximum: Decimal | None


@dataclass(frozen=True)
class PostLoadFacts:
    """Everything the checks compare, gathered in one pass."""

    fact_row_count: int
    airline_count_sum: int
    route_count_sum: int
    airline_bounds: tuple[AirlineFareBounds, ...]


def post_load_quality_check(engine: Engine | None = None) -> dict[str, Any]:
    """Run the KPI-level sanity checks; raise if any of them fails.

    Args:
        engine: injectable for tests; defaults to the analytics engine.

    Returns:
        A counts-only summary, safe for XCom: fact_row_count, airlines_checked,
        checks_run.

    Raises:
        QualityCheckError: one or more checks failed. The message names each
            failing check and the actual numbers involved.
    """
    engine = engine or get_analytics_engine()
    facts = fetch_post_load_facts(engine)
    failures = evaluate_post_load(facts)

    if failures:
        message = (
            f"Post-load quality check failed ({len(failures)} of 3 checks): "
            + " | ".join(failures)
        )
        logger.error(message)
        raise QualityCheckError(message)

    logger.info(
        "Post-load quality checks passed: %d fact rows, %d airlines, "
        "KPI sums and per-airline fare bounds all consistent.",
        facts.fact_row_count,
        len(facts.airline_bounds),
    )
    return {
        "fact_row_count": facts.fact_row_count,
        "airlines_checked": len(facts.airline_bounds),
        "checks_run": 3,
    }


def evaluate_post_load(facts: PostLoadFacts) -> list[str]:
    """Compare the snapshot; return one description per failing check.

    Pure: no database, no I/O. Returns an empty list when everything holds.
    """
    failures: list[str] = []

    failures.extend(
        _check_count_sum(
            KPI_OFFER_COUNT_TABLE, facts.airline_count_sum, facts.fact_row_count
        )
    )
    failures.extend(
        _check_count_sum(
            KPI_TOP_ROUTES_TABLE, facts.route_count_sum, facts.fact_row_count
        )
    )
    failures.extend(_check_fare_bounds(facts.airline_bounds))
    return failures


def _check_count_sum(table: str, kpi_sum: int, fact_row_count: int) -> list[str]:
    """Every fact row must be counted exactly once by the grouping.

    Holds because each row has exactly one airline and exactly one
    (source, destination) pair, all NOT NULL on the fact table — so the
    grouping partitions the fact table rather than sampling it.
    """
    if kpi_sum == fact_row_count:
        return []

    difference = kpi_sum - fact_row_count
    direction = "over-counts" if difference > 0 else "under-counts"
    return [
        (
            f"{table} sums to {kpi_sum} but {FACT_TABLE} holds "
            f"{fact_row_count} ({direction} by {abs(difference)})"
        )
    ]


def _check_fare_bounds(bounds: tuple[AirlineFareBounds, ...]) -> list[str]:
    """MIN <= AVG <= MAX per airline, plus airlines missing from either side.

    No tolerance is applied. The KPI average is ROUND(AVG(total_fare), 2) and
    the bounds are themselves NUMERIC(12,2) values drawn from the same column,
    so rounding can move the average by at most 0.005 and cannot carry it
    outside a bound that is already an exact 2dp value.
    """
    problems: list[str] = []

    for row in bounds:
        if row.average is None:
            problems.append(
                f"airline {row.airline!r} has rows in {FACT_TABLE} but no row "
                f"in {KPI_AVG_FARE_TABLE}"
            )
            continue

        if row.minimum is None or row.maximum is None:
            problems.append(
                f"airline {row.airline!r} appears in {KPI_AVG_FARE_TABLE} but "
                f"has no rows in {FACT_TABLE}"
            )
            continue

        if row.average < row.minimum:
            problems.append(
                f"airline {row.airline!r} average {row.average} is below its "
                f"minimum {row.minimum} (by {row.minimum - row.average})"
            )
        elif row.average > row.maximum:
            problems.append(
                f"airline {row.airline!r} average {row.average} is above its "
                f"maximum {row.maximum} (by {row.average - row.maximum})"
            )

    if not problems:
        return []

    # Reported as one failing check with every offending airline named, so the
    # message stays readable when a broken GROUP BY breaks all 24 at once.
    return [
        f"per-airline MIN <= AVG <= MAX violated for {len(problems)} airline(s): "
        + "; ".join(problems)
    ]


def fetch_post_load_facts(engine: Engine) -> PostLoadFacts:
    """Gather the snapshot the checks compare, in one transaction.

    One transaction so the counts cannot shift between queries and produce a
    failure that reflects timing rather than data.
    """
    with engine.begin() as conn:
        fact_row_count = int(
            conn.execute(text(f"SELECT COUNT(*) FROM {FACT_TABLE}")).scalar_one()
        )
        # COALESCE because SUM over an empty table is NULL, not 0 — without it
        # an empty KPI table would compare as None and mask the real finding.
        airline_count_sum = int(
            conn.execute(
                text(
                    f"SELECT COALESCE(SUM(flight_offer_count), 0) "
                    f"FROM {KPI_OFFER_COUNT_TABLE}"
                )
            ).scalar_one()
        )
        route_count_sum = int(
            conn.execute(
                text(
                    f"SELECT COALESCE(SUM(flight_offer_count), 0) "
                    f"FROM {KPI_TOP_ROUTES_TABLE}"
                )
            ).scalar_one()
        )
        # FULL OUTER JOIN so an airline present on only one side is visible.
        # An inner join would silently skip exactly the rows most worth seeing.
        rows = conn.execute(
            text(
                f"""
                SELECT COALESCE(f.airline, k.airline) AS airline,
                       f.min_fare,
                       k.avg_total_fare,
                       f.max_fare
                  FROM (SELECT airline,
                               MIN(total_fare) AS min_fare,
                               MAX(total_fare) AS max_fare
                          FROM {FACT_TABLE}
                         GROUP BY airline) f
                  FULL OUTER JOIN {KPI_AVG_FARE_TABLE} k
                    ON k.airline = f.airline
                 ORDER BY 1
                """
            )
        ).all()

    return PostLoadFacts(
        fact_row_count=fact_row_count,
        airline_count_sum=airline_count_sum,
        route_count_sum=route_count_sum,
        airline_bounds=tuple(
            AirlineFareBounds(
                airline=row[0], minimum=row[1], average=row[2], maximum=row[3]
            )
            for row in rows
        ),
    )
