"""
Flight price analysis pipeline — Airflow DAG.

This file orchestrates only: task definitions and dependencies. All business
logic lives in src/ and is imported here, per ADR-007 (see
docs/engineering_decisions.md). Task bodies below are placeholders until
Phase 0 dataset profiling (docs/data_profile.md) is complete — see
MASTER_PLAN.md for why nothing downstream is finalized before that.

Static, one-time dataset: schedule is None (manually triggered), not @daily —
see ADR-001. Do not add data_interval-based partitioning logic to this DAG.
"""

import pendulum

from airflow.sdk import DAG, task

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
        """Confirm the source CSV exists and is readable. Returns its path."""
        raise NotImplementedError("Pending Phase 0: confirm expected file path/name.")

    @task()
    def validate_source_schema(source_path: str) -> str:
        """File-level schema check: required headers present, parseable, no
        unexpected/missing columns — before any DB insertion is attempted."""
        raise NotImplementedError("Pending Phase 0: confirm actual CSV column set.")

    @task()
    def load_to_mysql_staging(source_path: str) -> str:
        """Truncate + bulk insert into staging.raw_flights. Returns pipeline_run_id."""
        raise NotImplementedError("Pending Phase 0 + include/sql/staging DDL.")

    @task()
    def validate_and_quarantine(pipeline_run_id: str) -> dict:
        """Level 2/3 row validation; writes rejects to staging.quarantine.
        Returns summary counts (small — safe for XCom)."""
        raise NotImplementedError("Pending docs/data_contract.md.")

    @task()
    def quality_gate_check(validation_summary: dict) -> None:
        """Raises if rejection rate exceeds the provisional threshold (ADR-005),
        which stops all downstream tasks via default trigger rules."""
        raise NotImplementedError("Pending Phase 0 rejection-rate baseline.")

    @task()
    def transform_and_load_fact(pipeline_run_id: str) -> str:
        """Compute Total Fare, normalize types, load valid rows into
        analytics.fact_flight_prices (name provisional — see ADR)."""
        raise NotImplementedError("Pending docs/data_contract.md and grain confirmation.")

    @task()
    def compute_kpi_avg_fare_by_airline(fact_table: str) -> None:
        raise NotImplementedError("Pending include/sql/analytics/kpi_avg_fare_by_airline.sql")

    @task()
    def compute_kpi_seasonal_fare_variation(fact_table: str) -> None:
        raise NotImplementedError(
            "Pending Phase 0 date-column finding — see docs/kpi_definitions.md."
        )

    @task()
    def compute_kpi_booking_count_by_airline(fact_table: str) -> None:
        raise NotImplementedError("Pending include/sql/analytics/kpi_booking_count_by_airline.sql")

    @task()
    def compute_kpi_top_routes(fact_table: str) -> None:
        raise NotImplementedError("Pending include/sql/analytics/kpi_top_routes.sql")

    @task()
    def post_load_quality_check(fact_table: str) -> None:
        """Row counts, nulls, key uniqueness, plus KPI-level reconciliation
        sanity checks (sum-of-counts, min<=avg<=max) — see MASTER_PLAN.md."""
        raise NotImplementedError("Pending fact table + KPI tables existing.")

    @task()
    def reconciliation_check(pipeline_run_id: str) -> None:
        """source_row_count == valid + rejected; valid == loaded — see
        staging.pipeline_runs audit table design in MASTER_PLAN.md."""
        raise NotImplementedError("Pending staging.pipeline_runs table.")

    source_path = check_source_file()
    validated_path = validate_source_schema(source_path)
    run_id = load_to_mysql_staging(validated_path)
    validation_summary = validate_and_quarantine(run_id)
    gate = quality_gate_check(validation_summary)
    fact_table = transform_and_load_fact(run_id)

    kpi_tasks = [
        compute_kpi_avg_fare_by_airline(fact_table),
        compute_kpi_seasonal_fare_variation(fact_table),
        compute_kpi_booking_count_by_airline(fact_table),
        compute_kpi_top_routes(fact_table),
    ]

    quality_check = post_load_quality_check(fact_table)
    for kpi_task in kpi_tasks:
        kpi_task >> quality_check

    gate >> fact_table
    quality_check >> reconciliation_check(run_id)
