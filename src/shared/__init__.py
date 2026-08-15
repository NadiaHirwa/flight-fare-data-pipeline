"""Code shared by more than one pipeline stage.

A module belongs here when two stages need the *identical* implementation and
neither should have to import the other's internals — src/transformation/
reaching into src/validation/ for a hash function would couple two stages that
are otherwise independent, and would make the import direction depend on which
one happened to be written first.

Nothing here may import from ingestion, validation, transformation, loading,
kpi, or quality: this package is a leaf, and keeping it one is what stops it
becoming a circular dependency.
"""
