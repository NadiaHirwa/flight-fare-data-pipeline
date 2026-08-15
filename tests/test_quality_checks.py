"""Unit tests for post_load_quality_check and reconciliation_check.

Both modules keep their comparison logic pure, so every failure mode is driven
by a hand-built fixture rather than a broken database.

The point of these tests is not that the checks pass on good data — a check
that always returns "fine" would do that too. Each check therefore has a
deliberately broken fixture proving it actually fails, and asserting that the
message names the real numbers, since "reconciliation failed" with no figures
is not actionable at 3am.
"""

from decimal import Decimal

import pytest

from src.quality.post_load import (
    FACT_TABLE,
    KPI_AVG_FARE_TABLE,
    KPI_OFFER_COUNT_TABLE,
    KPI_TOP_ROUTES_TABLE,
    AirlineFareBounds,
    PostLoadFacts,
    QualityCheckError,
    evaluate_post_load,
    post_load_quality_check,
)
from src.quality.reconciliation import (
    ReconciliationError,
    RunCounts,
    evaluate_reconciliation,
)


def bounds(airline, minimum, average, maximum):
    """An AirlineFareBounds row, taking plain strings for readability."""
    return AirlineFareBounds(
        airline=airline,
        minimum=None if minimum is None else Decimal(minimum),
        average=None if average is None else Decimal(average),
        maximum=None if maximum is None else Decimal(maximum),
    )


HEALTHY_BOUNDS = (
    bounds("Biman", "100.00", "300.00", "500.00"),
    bounds("US-Bangla", "200.00", "250.00", "900.00"),
)


def healthy_facts(**overrides):
    defaults = {
        "fact_row_count": 54478,
        "airline_count_sum": 54478,
        "route_count_sum": 54478,
        "airline_bounds": HEALTHY_BOUNDS,
    }
    return PostLoadFacts(**{**defaults, **overrides})


def healthy_counts(**overrides):
    defaults = {
        "pipeline_run_id": "manual__test",
        "source_row_count": 57000,
        "staged_row_count": 57000,
        "valid_row_count": 54478,
        "rejected_row_count": 2522,
        "loaded_row_count": 54478,
    }
    return RunCounts(**{**defaults, **overrides})


# ===========================================================================
# post_load_quality_check
# ===========================================================================

def test_healthy_snapshot_passes():
    assert evaluate_post_load(healthy_facts()) == []


def test_table_names_match_the_authoritative_definitions():
    """These names are restated in src/quality/ so a checker does not import
    the stages it audits. This pins them so they cannot drift silently."""
    from src.kpi import KPI_TABLES
    from src.transformation import FACT_TABLE as TRANSFORMATION_FACT_TABLE

    assert FACT_TABLE == TRANSFORMATION_FACT_TABLE
    for table in (KPI_AVG_FARE_TABLE, KPI_OFFER_COUNT_TABLE, KPI_TOP_ROUTES_TABLE):
        assert table in KPI_TABLES


# --- check 1: airline counts sum to the fact row count ---------------------

def test_airline_count_sum_too_low_fails():
    failures = evaluate_post_load(healthy_facts(airline_count_sum=54000))
    assert len(failures) == 1
    assert KPI_OFFER_COUNT_TABLE in failures[0]
    assert "54000" in failures[0]
    assert "54478" in failures[0]
    assert "under-counts by 478" in failures[0]


def test_airline_count_sum_too_high_fails():
    failures = evaluate_post_load(healthy_facts(airline_count_sum=54500))
    assert "over-counts by 22" in failures[0]


# --- check 2: route counts sum to the fact row count -----------------------

def test_route_count_sum_mismatch_fails():
    failures = evaluate_post_load(healthy_facts(route_count_sum=54477))
    assert len(failures) == 1
    assert KPI_TOP_ROUTES_TABLE in failures[0]
    assert "under-counts by 1" in failures[0]


def test_a_top_n_limit_in_the_routes_kpi_would_be_caught():
    """kpi_top_routes deliberately writes every route, not a top-N slice.
    If someone adds a LIMIT, this check is what notices."""
    failures = evaluate_post_load(healthy_facts(route_count_sum=500))
    assert failures
    assert KPI_TOP_ROUTES_TABLE in failures[0]


# --- check 3: per-airline MIN <= AVG <= MAX --------------------------------

def test_average_above_maximum_fails():
    broken = (*HEALTHY_BOUNDS, bounds("Novoair", "100.00", "999.00", "500.00"))
    failures = evaluate_post_load(healthy_facts(airline_bounds=broken))
    assert len(failures) == 1
    assert "Novoair" in failures[0]
    assert "above its maximum 500.00" in failures[0]
    assert "by 499.00" in failures[0]


def test_average_below_minimum_fails():
    broken = (bounds("Biman", "100.00", "50.00", "500.00"),)
    failures = evaluate_post_load(healthy_facts(airline_bounds=broken))
    assert "below its minimum 100.00" in failures[0]
    assert "by 50.00" in failures[0]


def test_average_exactly_on_the_bounds_passes():
    """min == avg == max is legitimate: one row, or every fare identical."""
    edge = (
        bounds("Single", "300.00", "300.00", "300.00"),
        bounds("AtMin", "100.00", "100.00", "500.00"),
        bounds("AtMax", "100.00", "500.00", "500.00"),
    )
    assert evaluate_post_load(healthy_facts(airline_bounds=edge)) == []


def test_airline_missing_from_the_kpi_table_fails():
    """Present in the fact table, absent from the KPI — a dropped group."""
    broken = (*HEALTHY_BOUNDS, bounds("Novoair", "100.00", None, "500.00"))
    failures = evaluate_post_load(healthy_facts(airline_bounds=broken))
    assert "Novoair" in failures[0]
    assert f"no row in {KPI_AVG_FARE_TABLE}" in failures[0]


def test_airline_missing_from_the_fact_table_fails():
    """In the KPI table but not the fact table — a stale KPI row that survived
    the TRUNCATE, or a KPI built against different data."""
    broken = (bounds("Ghost", None, "300.00", None),)
    failures = evaluate_post_load(healthy_facts(airline_bounds=broken))
    assert "Ghost" in failures[0]
    assert f"no rows in {FACT_TABLE}" in failures[0]


def test_every_offending_airline_is_named():
    broken = tuple(
        bounds(f"Airline{n}", "100.00", "999.00", "500.00") for n in range(3)
    )
    failures = evaluate_post_load(healthy_facts(airline_bounds=broken))
    assert len(failures) == 1, "reported as one failing check"
    assert "3 airline(s)" in failures[0]
    for n in range(3):
        assert f"Airline{n}" in failures[0]


# --- several at once -------------------------------------------------------

def test_all_three_checks_can_fail_together():
    """One run must report everything wrong, not just the first thing."""
    failures = evaluate_post_load(
        healthy_facts(
            airline_count_sum=1,
            route_count_sum=2,
            airline_bounds=(bounds("Biman", "100.00", "999.00", "500.00"),),
        )
    )
    assert len(failures) == 3


# --- the raising wrapper ---------------------------------------------------

class FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value

    def all(self):
        return self._value


class FakeConnection:
    """Returns queued results in order, recording the SQL it was given."""

    def __init__(self, results):
        self._results = list(results)
        self.queries = []

    def execute(self, statement, *args, **kwargs):
        self.queries.append(" ".join(str(statement).split()))
        return FakeResult(self._results.pop(0))


class FakeEngine:
    def __init__(self, results):
        self.connection = FakeConnection(results)

    def begin(self):
        engine = self

        class _Transaction:
            def __enter__(self):
                return engine.connection

            def __exit__(self, *exc):
                return False

        return _Transaction()


def fake_analytics(fact_rows, airline_sum, route_sum, rows):
    return FakeEngine([fact_rows, airline_sum, route_sum, rows])


def test_post_load_returns_a_summary_when_healthy():
    engine = fake_analytics(
        3, 3, 3, [("Biman", Decimal("100.00"), Decimal("150.00"), Decimal("200.00"))]
    )
    assert post_load_quality_check(engine=engine) == {
        "fact_row_count": 3,
        "airlines_checked": 1,
        "checks_run": 3,
    }


def test_post_load_raises_naming_the_failing_check():
    engine = fake_analytics(
        100, 90, 100, [("Biman", Decimal("1.00"), Decimal("2.00"), Decimal("3.00"))]
    )
    with pytest.raises(QualityCheckError) as excinfo:
        post_load_quality_check(engine=engine)
    message = str(excinfo.value)
    assert KPI_OFFER_COUNT_TABLE in message
    assert "90" in message
    assert "100" in message
    assert "under-counts by 10" in message


def test_post_load_never_passes_silently_on_a_mismatch():
    """The whole point: a wrong KPI must not return a cheerful summary."""
    engine = fake_analytics(100, 1, 1, [])
    with pytest.raises(QualityCheckError):
        post_load_quality_check(engine=engine)


# ===========================================================================
# reconciliation_check
# ===========================================================================

def test_healthy_counts_reconcile():
    assert evaluate_reconciliation(healthy_counts(), fact_row_count=54478) == []


# --- equation 1: source = valid + rejected ---------------------------------

def test_rows_unaccounted_for_fails():
    failures = evaluate_reconciliation(
        healthy_counts(rejected_row_count=2000), fact_row_count=54478
    )
    assert len(failures) == 1
    assert "source_row_count 57000" in failures[0]
    assert "54478" in failures[0]
    assert "2000" in failures[0]
    assert "522 row(s) unaccounted for" in failures[0]


def test_rows_counted_twice_fails():
    failures = evaluate_reconciliation(
        healthy_counts(rejected_row_count=3000), fact_row_count=54478
    )
    assert "478 row(s) counted twice" in failures[0]


# --- equation 2: valid = loaded --------------------------------------------

def test_valid_rows_never_loaded_fails():
    failures = evaluate_reconciliation(
        healthy_counts(loaded_row_count=54000), fact_row_count=54000
    )
    assert any("never loaded" in f for f in failures)
    assert any("54478" in f and "54000" in f for f in failures)


def test_more_loaded_than_valid_fails():
    failures = evaluate_reconciliation(
        healthy_counts(loaded_row_count=54500), fact_row_count=54500
    )
    assert any("loaded in excess" in f for f in failures)


# --- cross-check against the fact table ------------------------------------

def test_audit_row_disagreeing_with_the_fact_table_fails():
    """Both equations can hold while the served data is wrong — this is the
    check that makes the audit row accountable to reality."""
    failures = evaluate_reconciliation(healthy_counts(), fact_row_count=54000)
    assert len(failures) == 1
    assert FACT_TABLE in failures[0]
    assert "off by 478" in failures[0]


def test_an_empty_fact_table_is_caught():
    failures = evaluate_reconciliation(healthy_counts(), fact_row_count=0)
    assert failures
    assert "off by 54478" in failures[0]


# --- unusable audit rows ---------------------------------------------------

@pytest.mark.parametrize(
    "field",
    ["source_row_count", "valid_row_count", "rejected_row_count", "loaded_row_count"],
)
def test_missing_count_is_reported_not_crashed(field):
    """A NULL count means a stage never finished. Arithmetic on None would
    raise a TypeError that explains nothing."""
    failures = evaluate_reconciliation(
        healthy_counts(**{field: None}), fact_row_count=54478
    )
    assert len(failures) == 1
    assert field in failures[0]
    assert "did not complete" in failures[0]


def test_several_missing_counts_are_all_named():
    failures = evaluate_reconciliation(
        healthy_counts(valid_row_count=None, loaded_row_count=None),
        fact_row_count=0,
    )
    assert "valid_row_count" in failures[0]
    assert "loaded_row_count" in failures[0]


def test_staged_row_count_is_not_required():
    """It is not part of either documented equation."""
    assert evaluate_reconciliation(
        healthy_counts(staged_row_count=None), fact_row_count=54478
    ) == []


# --- several at once -------------------------------------------------------

def test_both_equations_and_the_cross_check_can_fail_together():
    failures = evaluate_reconciliation(
        healthy_counts(rejected_row_count=1, loaded_row_count=2),
        fact_row_count=3,
    )
    assert len(failures) == 3


# --- closing the audit row ------------------------------------------------

class FakeRunRow:
    """A pipeline_runs row that records the UPDATEs applied to it.

    Enough of a database to prove the status transition really happens: the
    run starts at 'running' (where every earlier stage leaves it) and must end
    at 'success' only once reconciliation has passed.
    """

    def __init__(self, counts):
        self.status = "running"
        self.completed_at = None
        self.counts = counts
        self.statements = []

    def apply(self, sql):
        self.statements.append(" ".join(sql.split()))
        if "status = 'success'" in sql:
            self.status = "success"
            self.completed_at = "set"
        elif "status = 'failed'" in sql:
            self.status = "failed"
            self.completed_at = "set"


class RunRowConnection:
    def __init__(self, row, fact_row_count):
        self.row = row
        self.fact_row_count = fact_row_count

    def execute(self, statement, *args, **kwargs):
        sql = str(statement)
        self.row.apply(sql)
        if "COUNT(*)" in sql:
            return FakeResult(self.fact_row_count)
        if "SELECT source_row_count" in sql:
            counts = self.row.counts
            return _OneRow(
                (
                    counts.source_row_count,
                    counts.staged_row_count,
                    counts.valid_row_count,
                    counts.rejected_row_count,
                    counts.loaded_row_count,
                )
            )
        return FakeResult(None)


class _OneRow:
    def __init__(self, values):
        self._values = values

    def one_or_none(self):
        return self._values


class RunRowEngine:
    def __init__(self, row, fact_row_count):
        self.connection = RunRowConnection(row, fact_row_count)

    def begin(self):
        engine = self

        class _Transaction:
            def __enter__(self):
                return engine.connection

            def __exit__(self, *exc):
                return False

        return _Transaction()


def test_status_transitions_from_running_to_success():
    """The audit row must not sit at 'running' forever after a clean run."""
    from src.quality.reconciliation import reconciliation_check

    row = FakeRunRow(healthy_counts())
    engine = RunRowEngine(row, fact_row_count=54478)

    assert row.status == "running"
    reconciliation_check("manual__test", staging_engine=engine, analytics_engine=engine)
    assert row.status == "success"
    assert row.completed_at == "set"


def test_status_stays_running_when_reconciliation_fails():
    """A failing run must not be recorded as successful."""
    from src.quality.reconciliation import reconciliation_check

    row = FakeRunRow(healthy_counts(rejected_row_count=2000))
    engine = RunRowEngine(row, fact_row_count=54478)

    with pytest.raises(ReconciliationError):
        reconciliation_check(
            "manual__test", staging_engine=engine, analytics_engine=engine
        )
    assert row.status == "running"
    assert row.completed_at is None


def test_success_is_recorded_on_the_staging_engine():
    """pipeline_runs lives with staging, not with the analytics layer."""
    from src.quality.reconciliation import reconciliation_check

    row = FakeRunRow(healthy_counts())
    staging = RunRowEngine(row, fact_row_count=54478)
    analytics = RunRowEngine(FakeRunRow(healthy_counts()), fact_row_count=54478)

    reconciliation_check(
        "manual__test", staging_engine=staging, analytics_engine=analytics
    )
    assert any("status = 'success'" in s for s in row.statements)
    assert not any(
        "status = 'success'" in s for s in analytics.connection.row.statements
    )


def test_reconciliation_error_is_not_a_quality_check_error():
    """Distinct types so a caller can tell which stage's invariant broke."""
    assert not issubclass(ReconciliationError, QualityCheckError)
    assert not issubclass(QualityCheckError, ReconciliationError)
