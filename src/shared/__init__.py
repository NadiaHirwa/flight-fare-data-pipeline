"""Code shared by more than one pipeline stage.

  connections.py    engines for the MySQL staging and PostgreSQL analytics
                    databases, resolved from Airflow Connections first and
                    .env second. No credentials in source.
  normalization.py  whitespace/casing normalization and the deterministic
                    record hash, so validation and transformation produce
                    identical digests.
  tables.py         staging table names and the CSV -> raw_flights column map.
  pipeline_runs.py  writers for the staging.pipeline_runs audit row, which
                    every stage updates part of.


A module belongs here when two stages need the *identical* implementation and
neither should have to import the other's internals — src/transformation/
reaching into src/validation/ for a hash function would couple two stages that
are otherwise independent, and would make the import direction depend on which
one happened to be written first.

Nothing here may import from ingestion, validation, transformation, loading,
kpi, or quality: this package is a leaf, and keeping it one is what stops it
becoming a circular dependency.
"""
