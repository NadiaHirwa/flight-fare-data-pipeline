"""
Flight price analysis pipeline — Airflow DAG.

This file orchestrates only: task definitions and dependencies. All business
logic lives in src/ and is imported here, per ADR-007 (see
docs/engineering_decisions.md). Each task body below is a thin call into that
logic — if a task grows a branch or a calculation, it belongs in src/.

Static, one-time dataset: schedule is None (manually triggered), not @daily —
see ADR-001, confirmed final after Phase 0 (57K rows, no reason to
reconsider). Do not add data_interval-based partitioning logic to this DAG.

XCom discipline (docs/MASTER_PLAN.md): every value passed between tasks here is
small — a path, a run id, or a counts-only summary dict. Row data never travels
through XCom; it lives in MySQL and PostgreSQL, and tasks pass references.
"""

import sys
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

# Make src/ importable. Derived from this file's own resolved location
# (dags/ -> project root), never from the process working directory: the DAG
# processor's cwd is not something this file can rely on, and without the
# bootstrap `import src` happens to work only because that cwd is currently
# /opt/airflow. An ImportError here would drop the whole DAG from the UI with
# an error pointing at the wrong thing.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion import (
    load_to_mysql_staging as run_load_to_mysql_staging,
)
from src.ingestion import (
    validate_source_schema as run_validate_source_schema,
)
from src.kpi import (
    compute_avg_fare_by_airline,
    compute_flight_offer_count_by_airline,
    compute_seasonal_fare_variation,
    compute_top_routes,
)
from src.quality import (
    post_load_quality_check as run_post_load_quality_check,
)
from src.quality import (
    quality_gate_check as run_quality_gate_check,
)
from src.quality import (
    reconciliation_check as run_reconciliation_check,
)
from src.transformation import (
    transform_and_load_fact as run_transform_and_load_fact,
)
from src.validation import (
    validate_and_quarantine as run_validate_and_quarantine,
)

SOURCE_FILE = PROJECT_ROOT / "include" / "data" / "Flight_Price_Dataset_of_Bangladesh.csv"

with DAG(
    dag_id="flight_price_pipeline",
    description="Ingest, validate, transform, and compute KPIs for the Bangladesh flight price dataset.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "hirwa",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["flight-price", "dem06"],
) as dag:

    @task()
    def check_source_file() -> str:
        """Confirm the source CSV exists and is readable. Returns its path.

        Deliberately has no src/ dependency: this is the one check that must
        work even if the project's own modules are broken, and "the file is
        missing" should not be reported as an import error.

        Per MASTER_PLAN's failure-strategy table, a missing source is
        fail-immediately, not retry — hence no retries on this task.
        """
        if not SOURCE_FILE.is_file():
            raise FileNotFoundError(
                f"Source CSV not found at {SOURCE_FILE}. The dataset is "
                "gitignored (size/licensing) — download it from Kaggle and "
                "place it in include/data/."
            )
        return str(SOURCE_FILE)

    @task()
    def validate_source_schema(source_path: str) -> str:
        """Level 1 file-level schema check, before any DB insertion.

        The underlying function returns True or raises; the path is passed
        through unchanged so the next task has it. A schema mismatch is
        fail-fast with no retry (MASTER_PLAN failure strategy) — re-reading the
        same bad file cannot produce a different answer.
        """
        run_validate_source_schema(source_path)
        return source_path

    @task()
    def load_to_mysql_staging(source_path: str, run_id: str | None = None) -> str:
        """Truncate + bulk insert into staging.raw_flights. Returns run_id.

        run_id is injected by Airflow from the task context — the DAG run's own
        identifier is reused as pipeline_run_id so a row in staging.pipeline_runs
        maps straight back to a run in the UI, with no extra lookup table.
        """
        if run_id is None:
            raise RuntimeError(
                "Airflow did not inject run_id into the task context; "
                "pipeline_run_id cannot be derived."
            )
        run_load_to_mysql_staging(source_path, run_id)
        return run_id

    @task()
    def validate_and_quarantine(pipeline_run_id: str) -> dict:
        """Level 2/3 row validation; writes rejects to staging.quarantine.

        Returns the counts-only summary (safe for XCom) that the gate consumes.
        """
        return run_validate_and_quarantine(pipeline_run_id)

    @task(retries=0)
    def quality_gate_check(validation_summary: dict) -> dict:
        """Fail the run if the rejection rate reaches the ADR-005 threshold.

        retries=0 overrides the DAG's default_args on purpose: this verdict is
        a deterministic comparison against a fixed set of counts, so a retry
        reaches the identical conclusion and only delays an inevitable failure
        by the retry_delay.

        Returns the summary unchanged so downstream tasks can read it without
        re-fetching validate_and_quarantine's XCom.
        """
        return run_quality_gate_check(validation_summary)

    @task()
    def transform_and_load_fact(pipeline_run_id: str) -> dict:
        """Convert valid rows and load analytics.flight_fare_quotes.

        Truncate-and-reload per ADR-001. Depends on quality_gate_check via an
        explicit edge below — it needs the gate to have passed, not the gate's
        return value.
        """
        return run_transform_and_load_fact(pipeline_run_id)

    @task()
    def compute_kpi_avg_fare_by_airline() -> dict:
        """KPI 1 — runs include/sql/analytics/kpi_avg_fare_by_airline.sql."""
        return compute_avg_fare_by_airline()

    @task()
    def compute_kpi_seasonal_fare_variation() -> dict:
        """KPI 4 — resolved via GROUP BY on the existing Seasonality column,
        see docs/kpi_definitions.md."""
        return compute_seasonal_fare_variation()

    @task()
    def compute_kpi_flight_offer_count_by_airline() -> dict:
        """KPI 2 — renamed from booking count (ADR-011): no booking entity
        exists in the source data."""
        return compute_flight_offer_count_by_airline()

    @task()
    def compute_kpi_top_routes() -> dict:
        """KPI 3 — runs include/sql/analytics/kpi_top_routes.sql."""
        return compute_top_routes()

    @task()
    def post_load_quality_check() -> dict:
        """KPI-level reconciliation: sum-of-counts and min<=avg<=max.

        Takes no argument — every table it inspects is named in
        docs/kpi_definitions.md. It depends on all four KPI tasks having run,
        which is expressed as explicit edges below rather than by threading a
        value none of the KPIs produce for it.
        """
        return run_post_load_quality_check()

    @task()
    def reconciliation_check(pipeline_run_id: str) -> dict:
        """source = valid + rejected; valid = loaded, per the pipeline_runs
        audit row, cross-checked against the fact table itself."""
        return run_reconciliation_check(pipeline_run_id)

    source_path = check_source_file()
    validated_path = validate_source_schema(source_path)
    run_id = load_to_mysql_staging(validated_path)
    validation_summary = validate_and_quarantine(run_id)
    gate = quality_gate_check(validation_summary)
    fact_table = transform_and_load_fact(run_id)

    kpi_tasks = [
        compute_kpi_avg_fare_by_airline(),
        compute_kpi_seasonal_fare_variation(),
        compute_kpi_flight_offer_count_by_airline(),
        compute_kpi_top_routes(),
    ]

    quality_check = post_load_quality_check()

    # Explicit edges, not data dependencies: the KPI tasks and the post-load
    # check need their upstreams to have *happened*, not to have returned
    # anything. Passing a value purely to create an edge would imply a data
    # flow that does not exist.
    gate >> fact_table
    for kpi_task in kpi_tasks:
        fact_table >> kpi_task >> quality_check

    quality_check >> reconciliation_check(run_id)
