"""
DAG structure tests — these check the DAG's shape (task IDs, dependencies),
not business logic. Safe to run before Phase 0 is complete, since @task
callables only execute when a task actually runs, not at import/parse time.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))

EXPECTED_TASK_IDS = {
    "check_source_file",
    "validate_source_schema",
    "load_to_mysql_staging",
    "validate_and_quarantine",
    "quality_gate_check",
    "transform_and_load_fact",
    "compute_kpi_avg_fare_by_airline",
    "compute_kpi_seasonal_fare_variation",
    "compute_kpi_booking_count_by_airline",
    "compute_kpi_top_routes",
    "post_load_quality_check",
    "reconciliation_check",
}


@pytest.fixture(scope="module")
def dag():
    from flight_price_pipeline_dag import dag as loaded_dag

    return loaded_dag


def test_dag_imports_without_error(dag):
    assert dag is not None


def test_expected_task_ids_present(dag):
    actual_ids = set(dag.task_dict.keys())
    assert actual_ids == EXPECTED_TASK_IDS, (
        f"Missing: {EXPECTED_TASK_IDS - actual_ids}, "
        f"Unexpected: {actual_ids - EXPECTED_TASK_IDS}"
    )


def test_quality_gate_precedes_fact_load(dag):
    gate = dag.get_task("quality_gate_check")
    assert "transform_and_load_fact" in gate.downstream_task_ids


def test_kpi_tasks_run_in_parallel(dag):
    """The four KPI tasks share the same upstream and downstream — none of
    them depend on each other, per the fan-out design in MASTER_PLAN.md."""
    kpi_ids = {
        "compute_kpi_avg_fare_by_airline",
        "compute_kpi_seasonal_fare_variation",
        "compute_kpi_booking_count_by_airline",
        "compute_kpi_top_routes",
    }
    for task_id in kpi_ids:
        t = dag.get_task(task_id)
        assert not (t.downstream_task_ids & kpi_ids), (
            f"{task_id} should not depend on another KPI task"
        )
        assert "transform_and_load_fact" in t.upstream_task_ids
        assert "post_load_quality_check" in t.downstream_task_ids


def test_dag_has_no_schedule_interval_partitioning(dag):
    """Static one-time dataset — ADR-001 — this DAG must not be @daily.
    schedule=None becomes a NullTimetable internally in Airflow 3.x."""
    assert type(dag.timetable).__name__ == "NullTimetable"