"""Transformation failure types.

A failure here is different in kind from a Level 2/3 validation failure. By the
time this stage runs, every row it sees has already passed the contract
(src/validation/), so a conversion that fails is not "bad data to quarantine" —
it means a row reached the fact load that validation should have stopped, or
the fact table and the contract have drifted apart. Either is a fault to
surface loudly, not to route around.
"""


class TransformationError(Exception):
    """Base for every transformation failure. Catch this to catch all of them."""


class RecordConversionError(TransformationError):
    """A contract-required value could not be converted to its declared type.

    Only raised for the nine nullable=false columns in docs/data_contract.md.
    The eight carried-through columns have no contract rule, so an unusable
    value there becomes NULL with a warning rather than failing the run — see
    convert_record.
    """


class FactLoadError(TransformationError):
    """The load into analytics.flight_fare_quotes did not complete correctly.

    Includes rows converted not matching rows landed, which would otherwise
    silently break the `valid_row_count = loaded_row_count` equation that
    reconciliation_check verifies.
    """
