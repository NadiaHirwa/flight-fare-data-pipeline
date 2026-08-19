# Testing Strategy

Status: **implemented.** 189 test functions across 7 test modules, all passing.
This document describes what exists, not what is planned.

```
Unit tests — no database, no Airflow required
    tests/test_ingestion.py        (24)  Level 1 file-schema validation: header set
                                         (order-tolerant, missing/extra rejected),
                                         duplicate headers, BOM, empty and
                                         header-only files, ragged rows, non-CSV input
    tests/test_validation.py       (63)  every Level 2/3 rule independently, the
                                         20-code IATA domain, the 1.00 tolerance
                                         boundary, exact-Decimal arithmetic,
                                         normalization, the record hash, and the
                                         batch-level duplicate grain-key check
    tests/test_transformation.py   (29)  string -> Decimal / datetime / int
                                         conversion, ROUND_HALF_UP, naive
                                         timestamps, strict vs lenient columns
    tests/test_quality_gate.py     (24)  the ADR-005 threshold: comfortably under,
                                         exactly at (must fail), just over, just
                                         under, and the real 4.42% figure
    tests/test_kpi.py              (13)  each KPI runner executes its own SQL file
                                         verbatim, in one transaction, counting its
                                         own table
    tests/test_quality_checks.py   (31)  post-load KPI sanity checks and the
                                         reconciliation equations, each with a
                                         deliberately broken fixture

DAG tests — require Airflow (run via `make test-docker`)
    tests/test_dag_integrity.py     (5)  DAG imports without error; expected task
                                         IDs; quality gate precedes the fact load;
                                         KPI tasks mutually independent (fan-out,
                                         not sequential); schedule is not
                                         interval-based (ADR-001)
```

**Every check has a failing case.** A check that only ever passes proves
nothing, so each validation rule is tested against data that violates it, the
quality gate is tested at and either side of its boundary, and both post-load
checks are driven by fixtures built to fail (mismatched counts, an airline whose
average falls outside its own min/max). The gate's boundary is 6% per ADR-005
— `gate.py` holds `REJECTION_RATE_THRESHOLD = 0.06`, and "exactly at threshold"
is asserted to **fail**, since ADR-005 specifies `>= 6%`.

**Route normalization and season classification are both implemented and
tested** — normalization in `src/shared/normalization.py` (whitespace stripped
everywhere, IATA codes upper-cased, casing preserved elsewhere so the
contract's exact value sets still mean something), and seasonality as a direct
accepted-value check against the four documented values.

**Database behaviour is verified by running the pipeline, not by mocked
integration tests.** Row/type outcomes in MySQL, the PostgreSQL fact table
state, and the reconciliation equations are all confirmed against the real
57,000-row dataset on a full DAG run — the results are recorded in
`docs/final_report.md` §7 and `docs/performance_metrics.md`. The unit tests
above deliberately avoid a live database so they stay fast and run in CI.

CI (`.github/workflows/ci.yml`) runs lint, DAG import validation, and the
test suite on every push/PR — intentionally lightweight, not a dedicated
CI/CD platform (that scope belongs to a different assignment).
