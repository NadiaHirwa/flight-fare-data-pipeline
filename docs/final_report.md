# Final Report — Flight Fare Data Pipeline

Status: outline only — to be written after implementation and a real run.
This is the graded deliverable; the sections below map directly to what the
lab specification asks for.

## 1. Pipeline architecture and execution flow

**Why two databases, not one.** The two layers need opposite guarantees, and one schema can't hold both. MySQL is the landing zone: it must accept everything. All 17 columns are `VARCHAR`, with no constraints. A malformed fare has to survive the load so quarantine can record what the source actually said — typing it as `DECIMAL` would make a bad row either crash the insert or get silently coerced, destroying the evidence the quarantine table exists to preserve. PostgreSQL is the serving layer: it must reject everything invalid — `NUMERIC(12,2)` never `FLOAT` (ADR-008), `NOT NULL` on contract columns, CHECK constraints, `UNIQUE` on the row hash, indexes on the KPI grouping keys. The same fare value needs to be permissive on the way in and strict on the way out; one database forces a choice, and either choice loses something — you can't quarantine what your schema already rejected.

Honest caveat: the assignment mandated two databases. This reasoning justifies the split; it didn't originate it. What was a deliberate design choice on top of that requirement was refusing a third "validated" table inside MySQL — valid rows are derived by an anti-join against `quarantine` and pass straight through to transformation, so nothing is ever stored twice.

**Why the DAG has the shape it does.** The linear head (`check_source_file` → `validate_source_schema` → `load_to_mysql_staging` → `validate_and_quarantine` → `quality_gate_check`) is real data dependency, not a stylistic choice — each task consumes the previous one's return value, and you can't validate a file you haven't staged yet.

The barrier at `transform_and_load_fact` exists because it truncates. Every KPI script is `INSERT INTO kpi_* SELECT ... FROM flight_fare_quotes`, and ADR-001 makes the fact load a full truncate-and-reload. A KPI task running concurrently with that truncate wouldn't crash — it would aggregate a half-loaded table, write a plausible-looking number, and report success. That is the dangerous failure mode this barrier prevents: silently wrong, rather than loudly broken. The barrier converts a race condition into a guaranteed ordering.

The four KPI tasks run in parallel because no dependency between them is real — each reads the same fact table and writes its own disjoint table, and none reads another's output. Serializing them would encode a dependency that doesn't exist. The payoff isn't speed (each task runs in roughly 0.4 seconds, so parallelism saves about a second) — it's failure isolation, and this project produced a live demonstration of it: when `compute_kpi_top_routes` hit a table-ownership permissions bug, the other three KPI tasks still ran and succeeded in the same DAG run. That meant learning, in a single run, that exactly one KPI was broken and three were fine — chained sequentially, the first failure would have blocked the rest, and the same bug would only have been discovered three runs later, one at a time.

The fan-in at `post_load_quality_check` is a genuine barrier for the same reason as the first one: it compares `SUM(kpi counts)` against the fact table's row count, and running it before every KPI task has finished would mean reading a partially-built KPI table and failing on timing rather than on the data itself.

One deliberate detail: both the fan-out and the fan-in use explicit `>>` edges, not a value threaded through as task input/output. The original DAG skeleton passed a `fact_table` string purely to create the dependency edge, but that implied a data flow none of these tasks actually needed — they require their upstream task to have *happened*, not to have *returned* something.

## 2. Description of each Airflow DAG and task

**`check_source_file` / `validate_source_schema`.** Both failures ultimately mean "the file is unusable," so folding them into one task looks obvious — but they're kept separate because they have different owners. A missing file is an operations problem: nobody dropped the CSV in `include/data/`, or a volume mount broke. A wrong header set is a data-contract problem: the source changed shape, and someone has to decide whether `docs/data_contract.md` moves or the file gets rejected. Different person, different fix, different urgency. Kept separate, the Airflow UI is the diagnosis — one red square tells you which of those two worlds you're in before you open a log; merged, every failure is just "the source task went red." `check_source_file` also deliberately imports nothing from `src/`, so it's the only task that still works even if the project's own modules are broken — if `src/ingestion` had an import error, a merged task would report a missing CSV as an `ImportError`, the least useful possible framing.

**`load_to_mysql_staging`.** Truncates `raw_flights` and bulk-loads the CSV, assigning each row's `source_row_number` from its position in the file — the value `quarantine.source_row_number` later reads from.

**`validate_and_quarantine` / `quality_gate_check`.** The strongest case for splitting in the whole DAG, and the reason is mechanical: Airflow configures retries per task. `validate_and_quarantine` inherits the DAG's default `retries: 1`, correctly, since it does 57,000 rows of real database work and a transient MySQL blip deserves another attempt. `quality_gate_check` is set to `retries=0`, because its verdict is a comparison of fixed integers against a 6% threshold — retrying reaches the identical conclusion five minutes later. Folded together, those two retry requirements are irreconcilable. Underneath that is a cleaner split: measuring versus judging. `validate_and_quarantine` records what happened; the gate applies a policy that ADR-005 already changed once, from 5% to 6%. Policy that's expected to change shouldn't live inside the code that measures it — practically, changing the threshold means clearing `quality_gate_check` and re-running one task, not re-validating 57,000 rows to re-apply a `>=`. Quarantine rows are also written before the gate can fail the run, so when the rejection rate spikes, the evidence for why is already on disk.

**`transform_and_load_fact`.** Worth describing where splitting was deliberately *rejected*, since the principle isn't "split everything" — ADR-007 explicitly rejects one-task-per-function. Converting types and loading into Postgres look like two separable steps, but two things make that boundary impossible here. The intermediate result is 54,478 fully-typed rows, and passing that between tasks would violate the project's XCom discipline (small values only, real data stays in a database) — the only alternative would be persisting it somewhere, recreating exactly the redundant "validated copy" the two-database design already refused to have. More decisively, the table truncate and the row inserts share a single database transaction, so a mid-load failure rolls back to the previous good contents instead of leaving the analytics layer observably empty — and a transaction cannot span two Airflow tasks. Splitting here wouldn't just be less elegant; it would forfeit the guarantee that served data is never half-written.

**The four `compute_kpi_*` tasks.** Each executes its own already-verified SQL file from `include/sql/analytics/` — `avg_fare_by_airline`, `flight_offer_count_by_airline`, `top_routes`, `seasonal_fare_variation`. Why they run as four separate parallel tasks rather than one combined task is covered in §1 (failure isolation, demonstrated live when one KPI's table-ownership bug didn't block the other three).

**`post_load_quality_check`.** Runs the KPI-level sanity checks — confirming `SUM(flight_offer_count_by_airline)` and `SUM(top_routes counts)` both equal the fact table's row count, and that `MIN(total_fare) <= AVG(total_fare) <= MAX(total_fare)` holds for every airline. This catches a KPI table that exists and is non-null but is logically wrong — a different failure class than a missing table.

**`reconciliation_check`.** The last task. Verifies both documented equations (`source = valid + rejected`, `valid = loaded`) against `pipeline_runs`, plus a third check added beyond what was originally specified: comparing `loaded_row_count` against the fact table's actual `COUNT(*)`, since the first two equations are computed entirely from numbers the pipeline reported about itself — a stage that miscounted would satisfy both while still having lost rows. Only once all three hold does this task mark the run `status='success'` in `pipeline_runs` — before this was added, a fully successful run stayed at `'running'` forever, since no other task had standing to make that call.

## 3. ETL/ELT classification

By the time a row reaches PostgreSQL, all the work is already done. `transform_and_load_fact` pulls the valid rows out of MySQL as plain strings, and in Python it turns `"21131.22502141266"` into a proper decimal of `21131.23`, turns `"2025-11-17 06:25:00"` into a real timestamp, tidies the airport codes, and computes the row's hash. Then it inserts. So Postgres never sees a half-finished row — it only ever receives data that's already the right shape. That's the whole thing that makes this ETL: you transform, and then you load. If it were the other way round, you'd be dumping raw text into Postgres and cleaning it up in place afterwards.

It's tempting to say "we loaded into MySQL, so that's the L" — but look at what actually happens there. Every column is `VARCHAR`. The value goes in exactly as the file wrote it, character for character. Nothing is converted, nothing is cleaned, nothing is calculated — deliberately, so a broken value survives long enough to be quarantined with its original text intact. Landing a copy of the file somewhere isn't transforming it. MySQL is a parking space, not a workshop. The load that matters — the one the ETL/ELT question is actually about — is the one into the database people will query.

The four KPI tasks follow a different pattern entirely: they're ELT. None of them does any work in Python — each one just opens its SQL file and hands it to Postgres, and the SQL says roughly "read `flight_fare_quotes`, group it, write the result into this KPI table." The data never leaves the database; Postgres does the aggregating itself, on rows it's already holding. That's the opposite order: the data was loaded first, and then transformed, by the destination, in place.

So, plainly: the pipeline is mostly ETL, with an ELT tail stuck on the end, and the line between them is the fact table. Everything before it is transform-then-load; everything after it is load-then-transform. Calling it just "a hybrid" would be true but lazy — the interesting answer is knowing exactly where the switch happens and why.

## 4. KPI definitions and computation logic

**Average Fare by Airline** — what a typical ticket costs on each of the 24 airlines. It's the "who's expensive, who's cheap" question. Turkish Airlines sits at the top around 74,170 BDT, and the spread across airlines is honestly pretty narrow.

**Flight Offer Count by Airline** — how many listings each airline has in the dataset. Worth being careful with this one: it does not mean how many tickets anyone sold. There's no booking or customer anywhere in this data, so it's "how much of the catalogue is theirs," not "how popular are they." That's exactly why it got renamed from the assignment's "Booking Count" — calling it bookings would have been quietly making something up.

**Top Routes** — which city pairs show up most, like DAC–CGP or RJH–SIN, counted across all 152 routes in the data.

**Seasonal Fare Variation** — whether fares move depending on the time of year: Regular, Winter Holidays, Hajj, or Eid.

**How the seasonal KPI nearly didn't happen.** Going in, this was the one KPI that might have been impossible. To do "seasonal variation" properly you need to know which dates count as peak — and nobody knew whether the file even had a date in it. If it didn't, the plan was to either redefine the KPI on whatever was closest, or say honestly that it couldn't be built. What was explicitly refused was inventing our own peak-season date ranges and pretending the data supported them.

Then profiling opened the file, and it turned out there was not just a date column but an actual `Seasonality` column already sitting there — every row already labelled Regular, Winter Holidays, Hajj, or Eid. The whole problem evaporated. The KPI became a straight "group by that column and average the fare." No date maths, no invented boundaries, no assumptions.

The slightly counterintuitive lesson: deriving our own date windows would have looked more impressive on paper. But it would have been our guess layered on top of someone else's answer. The simpler version is the more defensible one.

**The number worth quoting.** Hajj fares average 94,632 BDT against 67,018 for Regular — about 41% higher. Eid is close behind at 90,549, Winter Holidays at 78,739. The season ranking isn't just noise, it's a clean ladder.

What makes it more interesting: Hajj is the smallest group in the whole dataset — around 942 rows out of 57,000, under 2%. The strongest signal in the data lives in its thinnest slice, which is easy to miss if you only look at averages across everything.

One honest counterweight, from the same family of results: the Top Routes numbers are almost flat — 152 routes spanning only 299 to 399 offers each. RJH–SIN technically "wins," but at that spread it's barely a winner at all. Reporting it as the standout route would be reading meaning into noise. The flatness is the actual finding.

## 5. Engineering decisions

Eleven ADRs are recorded in full in [`engineering_decisions.md`](engineering_decisions.md). Four stand out, in rough order of how much they actually shaped the build:

**ADR-010 — validate against an independent reference domain.** The obvious way to check "invalid city names" is to collect the distinct values from the CSV and compare each row against them — which is circular and can never fail. Citing the IATA registry instead turned a check that tested nothing into one that would genuinely catch a bad code in a future file. The best single piece of judgment in the project.

**ADR-003 — three-level validation with quarantine, never silent drops.** Structurally the biggest: it produced the quarantine table, the "valid rows are an anti-join, not a second table" design, the quality gate, and the reconciliation equations. Almost everything downstream is a consequence of this one decision.

**ADR-008 — money as `NUMERIC(12,2)`, never `FLOAT`.** Sounds like a footnote, but the Total Fare reconciliation turned out to be the only rule that ever fires (all 2,522 rejections). That check compares a difference against a 1.00 tolerance — with binary floats, the rounding error would sit inside the very comparison meant to detect bad arithmetic, and the headline data-quality finding would have been untrustworthy.

**ADR-001 — truncate-and-reload for a static dataset.** Rejecting Airflow's date-partitioning default because there's no "yesterday's slice" here. It's why every task is retry-safe by construction, and why the fact load can be one atomic transaction that rolls back cleanly.

A fifth worth including cheaply: **ADR-011** (renaming to `flight_fare_quotes` and "Flight Offer Count") is the honesty one — the data has no booking entity, so calling it bookings would have misrepresented every count in the report.

## 6. Challenges encountered and how they were resolved

### The city-validation check that couldn't fail

The assignment says to flag invalid city names. The obvious implementation is to read the distinct Source/Destination values out of the CSV and check every row against that set — and it's completely worthless, because you're comparing the file to itself. It can never fail. You'd tick the requirement and test nothing.

This one got caught by thinking rather than debugging, before any code existed. The fix was to go get an external authority: the 20 real IATA codes, confirmed against the official registry and written into the data contract, with 8 Bangladesh domestic codes valid as origin and all 20 valid as destination. Phase 0 later confirmed the file's own values happen to be exactly that set — but the check now points at something outside the file, so a genuinely wrong code in a future extract still gets caught.

The lesson that stuck: a passing check isn't evidence of anything until you know what would make it fail.

### Airflow 3 moved the furniture

The DAG was finished and correct long before it could actually run. Three failures, one after another, none of them in the pipeline code:

First, nothing was registered at all — `airflow dags list` said "No data found" and triggering failed with `DagNotFound`, even though the file parsed cleanly and the integrity tests passed. Turned out Airflow 3 pulled DAG parsing out of the scheduler into a separate `dag-processor` service that the compose file didn't run.

Then tasks started dying instantly at "Pre Execute" with `[Errno 111] Connection refused`. In Airflow 3 a running task calls back to an Execution API instead of touching the metadata DB directly, and the default URL is `localhost:8080` — which is nothing inside the scheduler container.

Fix that, and it became `Invalid auth token`. That callback is authenticated with a JWT, and each process was generating its own signing secret.

The thing that made this tractable was that the error message changed every time. Same symptom three times over would have meant flailing; a new error each round meant each fix was real and there was another layer underneath. All three now live in `docker-compose.yml`. Zero DAG bugs — a major version bump had quietly relocated the runtime contract.

### Grants aren't ownership

The least-privilege `analytics_writer` role couldn't use its own tables. First it was plain `permission denied` — the init script granted on the schema, which doesn't cover tables created in it later. Granted table privileges, and reads and writes started working.

Then one task still failed: `must be owner of table kpi_top_routes`. The `CREATE INDEX` in that script needs ownership, and no amount of granting ever provides that.

Two things made this a good bug. It was found by the pipeline rather than by reading anything — specifically because the four KPI tasks run in parallel, so three went green and exactly one went red. The failing task named the table in its own error message.

And the first fix was wrong. Transferring ownership of the five existing tables worked, but it was a patch on a live database that a fresh clone would hit all over again. The real fix was making `analytics_writer` own the `public` schema so it creates and therefore owns its own tables, plus `ALTER DEFAULT PRIVILEGES` for anything the superuser adds later — then destroying the volume and rebuilding to prove it.

### "It works from scratch" kept being untrue

The stated goal was clone → configure → up → triggerable, no undocumented steps. Every time that claim got tested honestly — `make clean`, destroy every volume, start from nothing — another hidden manual step fell out.

First run: the DDL had never actually been applied by anything; it had just been done by hand at some point. Second run: the two Airflow Connections were gone with the metadata volume, and had been created by hand too.

Both are automated now — `airflow-init` creates the Connections, `make db-init` applies every schema file as the correct least-privilege role. But the pattern is the real lesson: nobody was lying about reproducibility, it just quietly decayed as things got fixed interactively. The only way to know is to destroy everything and try it, and then do that again after the next change.

## 7. Reconciliation and data quality results

The five headline numbers, from run `manual__zero_setup_001`:

```
source_row_count    57,000
staged_row_count    57,000
valid_row_count     54,478
rejected_row_count   2,522   (4.42%)
loaded_row_count    54,478
```

Run finished with `status = success`.

**All three equations held:**

```
57,000 = 54,478 + 2,522                      source = valid + rejected     ✓
54,478 = 54,478                              valid = loaded                ✓
54,478 = COUNT(*) in flight_fare_quotes      loaded = what's actually there ✓
```

The third one isn't in the original specification. It got added because the first two are calculated entirely from figures the pipeline reported about itself — a stage that miscounted would satisfy both while still having quietly lost rows. Reading the fact table directly is what makes the audit row answerable to reality rather than just internally consistent.

**One rule accounted for every single rejection:**

```
rule_violated               level    rows
total_fare_reconciliation       3   2,522
```

That's `abs(total_fare - (base_fare + tax_surcharge)) > 1.00` — rows where the total doesn't match what the base fare and tax add up to. Nothing else fired at all. No nulls, no zero or negative fares, no invalid IATA codes, no unrecognised fare classes or seasonality values, no duplicate rows on the grain key. Every other rule in the contract ran against all 57,000 rows and found nothing.

**What that actually tells you.** Mostly that the dataset is clean apart from one deliberately broken thing. The 4.42% matches Phase 0's independent profiling of the raw file exactly, which is a good sign — the pipeline found precisely what an entirely separate pass had already measured, no more and no less.

And the failures aren't rounding noise. The gaps run from roughly 445 to 93,165 BDT, so these are badly wrong totals, not floating-point dust. That's also why the 1.00 tolerance is set where it is — it covers 2-decimal rounding and nothing else, and widening it wouldn't rescue a single genuine row.

The rate is also flat across every season and every class, which is what you'd expect from noise injected at random rather than a systematic calculation bug in the source. It sits comfortably under the 6% quality gate with about 1.6 percentage points of headroom.

Worth adding: none of the 2,522 rejected rows were thrown away. They're all in `staging.quarantine` with their original values, their exact row number in the CSV, the rule that caught them, and a readable reason. A separate check confirmed none of those rows made it into the fact table.