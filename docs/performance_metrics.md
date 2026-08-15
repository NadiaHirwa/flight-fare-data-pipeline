# Performance Metrics

Status: **populated from a real run.**

```
pipeline_run_id : manual__zero_setup_001
date            : 2026-08-15 12:37:37 UTC
dag_run state   : success (all 12 tasks, no retries)
source file     : Flight_Price_Dataset_of_Bangladesh.csv (13.49 MB)
sha256          : 0389ea8c87dfa94cc9eebae7f344f913799504845c238bcaad4577738ef8718f
```

Measured on a container stack rebuilt from scratch (`make clean && make up`),
against the real 57,000-row dataset. Task durations are Airflow's own
`task_instance.duration`; counts come from `staging.pipeline_runs`, which is
the audit row the pipeline writes about itself.

## Timings

```
Pipeline duration (total):                       32.43 s
Sum of task durations:                           27.86 s

Per-task duration:
    check_source_file                             4.31 s
    validate_source_schema                        0.27 s
    load_to_mysql_staging                         3.38 s
    validate_and_quarantine                       2.83 s
    quality_gate_check                            0.32 s
    transform_and_load_fact                      14.34 s
    compute_kpi_avg_fare_by_airline               0.41 s
    compute_kpi_seasonal_fare_variation           0.41 s
    compute_kpi_flight_offer_count_by_airline     0.45 s
    compute_kpi_top_routes                        0.46 s
    post_load_quality_check                       0.31 s
    reconciliation_check                          0.37 s
```

The 4.57 s between the total and the sum of task durations is scheduler
overhead — queueing and slot assignment between tasks, not work.

`check_source_file` at 4.31 s is misleading as a measure of the check itself:
it is the first task of the run and absorbs the cold start of the worker
process. Every later task runs against a warm interpreter.

## Records

```
Records processed (source rows):                 57,000
Records staged (staging.raw_flights):            57,000
Records rejected (staging.quarantine):            2,522
Rejection rate:                                    4.42 %
Records loaded (analytics.flight_fare_quotes):   54,478
```

Every rejection came from one rule:

```
rule_violated               level   rows
total_fare_reconciliation       3   2,522
```

That is the dataset's known noise floor, not a pipeline fault — Phase 0
measured the same 4.42 % (`docs/data_profile.md`), and ADR-005 sets the quality
gate at 6 % specifically to sit above it. No other rule fired: no nulls, no
non-positive fares, no invalid IATA codes, no duplicate grain keys.

Reconciliation held on both documented equations:

```
source_row_count  57,000 = valid 54,478 + rejected 2,522   ✓
valid_row_count   54,478 = loaded_row_count 54,478          ✓
loaded_row_count  54,478 = COUNT(*) in flight_fare_quotes   ✓
```

KPI tables produced: 24 airlines, 24 airline counts, 152 routes, 4 seasons.

## Throughput

Derived as rows ÷ the duration of the task that moved them.

```
Staging load  (CSV -> MySQL):        57,000 / 3.38 s  = 16,864 rows/sec
Validation    (Levels 2/3):          57,000 / 2.83 s  = 20,141 rows/sec
Fact load     (MySQL -> PostgreSQL): 54,478 / 14.34 s =  3,799 rows/sec
End-to-end    (source rows):         57,000 / 32.43 s =  1,758 rows/sec
```

The fact load is the slowest stage by a wide margin, and legitimately so: it is
the only stage doing per-row work in Python — Decimal conversion, timestamp
parsing and a SHA-256 hash per row — and it writes across a database boundary,
MySQL to PostgreSQL. The staging load moves more rows in a quarter of the time
because it treats every value as text and never leaves MySQL.

## Notes on reading these numbers

They describe a single-machine Docker stack with `LocalExecutor`, all four
services competing for the same host CPU. They are useful as a baseline for
"has a change made this materially slower", not as a capacity estimate.

At this size the whole pipeline finishes in about half a minute, which is the
substantive point behind ADR-001: truncate-and-reload is not a compromise at
57,000 rows, it is simply the cheapest correct option.
