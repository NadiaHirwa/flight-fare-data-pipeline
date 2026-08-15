"""Unit tests for the KPI runners.

These confirm each function executes the *right* SQL file, in one transaction,
and reports the row count. They deliberately do not re-verify the SQL logic —
that is proven by the reconciliation checks in docs/kpi_definitions.md and was
exercised end-to-end against the real dataset.

The engine is a small hand-written fake rather than MagicMock, so the
assertions can say exactly what was executed rather than that some method was
called with something.
"""

import pytest

from src.kpi import (
    KPI_TABLES,
    SQL_DIR,
    KpiError,
    compute_avg_fare_by_airline,
    compute_flight_offer_count_by_airline,
    compute_seasonal_fare_variation,
    compute_top_routes,
)
from src.kpi import runner as kpi_runner

# (function, expected table). The table is also the script's filename stem.
KPI_FUNCTIONS = [
    (compute_avg_fare_by_airline, "kpi_avg_fare_by_airline"),
    (compute_flight_offer_count_by_airline, "kpi_flight_offer_count_by_airline"),
    (compute_top_routes, "kpi_top_routes"),
    (compute_seasonal_fare_variation, "kpi_seasonal_fare_variation"),
]


class FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        return self._value


class FakeConnection:
    def __init__(self, row_count):
        self._row_count = row_count
        self.scripts = []      # every exec_driver_sql payload, in order
        self.queries = []      # every execute() statement, as text

    def exec_driver_sql(self, sql):
        self.scripts.append(sql)

    def execute(self, statement, *args, **kwargs):
        self.queries.append(str(statement))
        return FakeResult(self._row_count)


class FakeEngine:
    """Records what was executed, and how many transactions were opened."""

    def __init__(self, row_count=0):
        self.connection = FakeConnection(row_count)
        self.transactions = 0

    def begin(self):
        self.transactions += 1
        engine = self

        class _Transaction:
            def __enter__(self):
                return engine.connection

            def __exit__(self, *exc):
                return False

        return _Transaction()


# ---------------------------------------------------------------------------
# The scripts exist and the constant matches them
# ---------------------------------------------------------------------------

def test_sql_dir_resolves_to_the_repository_scripts():
    assert SQL_DIR.is_dir(), f"SQL_DIR does not exist: {SQL_DIR}"


@pytest.mark.parametrize("table", KPI_TABLES)
def test_every_kpi_table_has_a_script(table):
    assert (SQL_DIR / f"{table}.sql").is_file()


def test_kpi_tables_matches_the_scripts_on_disk():
    """Guards against a script being added or renamed without the runner."""
    on_disk = {p.stem for p in SQL_DIR.glob("kpi_*.sql")}
    assert on_disk == set(KPI_TABLES)


def test_there_are_four_kpis():
    """docs/kpi_definitions.md defines exactly four."""
    assert len(KPI_TABLES) == 4


# ---------------------------------------------------------------------------
# Each function runs its own script
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(("compute", "table"), KPI_FUNCTIONS)
def test_function_executes_its_own_script_verbatim(compute, table):
    """The exact file content must reach the driver, unmodified."""
    engine = FakeEngine(row_count=24)
    compute(engine=engine)

    expected = (SQL_DIR / f"{table}.sql").read_text(encoding="utf-8")
    assert engine.connection.scripts == [expected]


@pytest.mark.parametrize(("compute", "table"), KPI_FUNCTIONS)
def test_function_counts_its_own_table(compute, table):
    engine = FakeEngine(row_count=24)
    compute(engine=engine)
    assert engine.connection.queries == [f"SELECT COUNT(*) FROM {table}"]


@pytest.mark.parametrize(("compute", "table"), KPI_FUNCTIONS)
def test_function_returns_the_row_count(compute, table):
    engine = FakeEngine(row_count=17)
    assert compute(engine=engine) == {"kpi": table, "row_count": 17}


def test_each_function_runs_a_different_script():
    """Anti-copy-paste guard: four functions, four distinct scripts.

    Without this, a duplicated call target would still pass every per-function
    test above while silently computing one KPI four times.
    """
    executed = []
    for compute, _ in KPI_FUNCTIONS:
        engine = FakeEngine()
        compute(engine=engine)
        executed.append(engine.connection.scripts[0])

    assert len(set(executed)) == 4


# ---------------------------------------------------------------------------
# How it executes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(("compute", "table"), KPI_FUNCTIONS)
def test_script_is_not_split_on_semicolons(compute, table):
    """Regression guard.

    These scripts contain semicolons inside comments, so splitting on ";" cuts
    a statement in half. The whole file must go to the driver in exactly one
    call, which is what psycopg2, psql and the Postgres operator all expect.
    """
    engine = FakeEngine()
    compute(engine=engine)

    assert len(engine.connection.scripts) == 1
    script = engine.connection.scripts[0]
    assert script.count(";") > 1, "fixture should contain several statements"
    assert "CREATE TABLE IF NOT EXISTS" in script
    assert "TRUNCATE TABLE" in script
    assert "INSERT INTO" in script


@pytest.mark.parametrize(("compute", "table"), KPI_FUNCTIONS)
def test_write_and_count_share_one_transaction(compute, table):
    """The reported count must describe what this run wrote."""
    engine = FakeEngine()
    compute(engine=engine)
    assert engine.transactions == 1


@pytest.mark.parametrize(("compute", "table"), KPI_FUNCTIONS)
def test_no_kpi_logic_is_restated_in_python(compute, table):
    """The runner must not build SQL of its own beyond the row count.

    docs/kpi_definitions.md keeps the definition in include/sql/analytics/; a
    second copy in Python would be free to drift from it.
    """
    engine = FakeEngine()
    compute(engine=engine)
    for query in engine.connection.queries:
        assert query.startswith("SELECT COUNT(*) FROM ")


# ---------------------------------------------------------------------------
# Failure
# ---------------------------------------------------------------------------

def test_missing_script_raises_kpi_error(monkeypatch, tmp_path):
    monkeypatch.setattr(kpi_runner, "SQL_DIR", tmp_path)
    with pytest.raises(KpiError) as excinfo:
        compute_top_routes(engine=FakeEngine())
    message = str(excinfo.value)
    assert "kpi_top_routes" in message
    assert str(tmp_path) in message


def test_missing_script_does_not_touch_the_database(monkeypatch, tmp_path):
    """Fail before opening a transaction, not halfway through one."""
    monkeypatch.setattr(kpi_runner, "SQL_DIR", tmp_path)
    engine = FakeEngine()
    with pytest.raises(KpiError):
        compute_top_routes(engine=engine)
    assert engine.transactions == 0
    assert engine.connection.scripts == []
