# Testing Strategy

```
Unit tests (tests/)
    - fare/total calculation and tolerance check
    - each validation rule (Level 1/2/3), independently
    - route normalization, season classification (if applicable)

Database/integration tests
    - MySQL ingestion produces expected row/type outcomes
    - Postgres load matches expected fact table state
    - reconciliation equations hold against a known fixture dataset

DAG tests (tests/test_dag_integrity.py — already implemented, structure-only)
    - DAG imports without error
    - expected task IDs and dependency structure
    - KPI tasks are mutually independent (fan-out, not sequential)

Data quality tests
    - not-null / uniqueness / accepted-value checks against docs/data_contract.md
    - quality-gate threshold boundary cases (just under / just over 5%)
    - KPI reconciliation sanity checks (see docs/kpi_definitions.md)
```

CI (`.github/workflows/ci.yml`) runs lint, DAG import validation, and the
test suite on every push/PR — intentionally lightweight, not a dedicated
CI/CD platform (that scope belongs to a different assignment).
