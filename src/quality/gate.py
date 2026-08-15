"""Data-quality gate — the rejection-rate threshold from ADR-005.

Implements the quality_gate_check task body (ADR-007). Sits between
validate_and_quarantine and transform_and_load_fact: it decides whether a
batch is normal enough to serve, having already let individual bad rows be
quarantined and continue.

This is the one failure mode in docs/MASTER_PLAN.md's failure-strategy table
that is deliberately NOT quarantine-and-continue:

    A few invalid rows (Level 2/3)  -> quarantine + continue
    Rejection rate over threshold   -> quality_gate_check fails the pipeline

The distinction is the whole point of the gate. Quarantining rows is the
correct response to a handful of bad records; it is the wrong response to a
batch that is broadly wrong, because the result would be a fact table quietly
missing a large share of its data while every task reported success. So this
raises, the task fails, and downstream tasks do not run under Airflow's
default all_success trigger rule.

Standard library only — no database, no Airflow. The gate is a pure decision
over a counts-only summary, so it is fully testable without infrastructure.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# docs/engineering_decisions.md, ADR-005: "Rejection rate `< 6%` -> continue
# with a warning. `>= 6%` -> fail the quality gate."
#
# Deliberately not 5%. Phase 0 measured this dataset's own natural noise floor
# at 4.42% — the Total Fare reconciliation rule, with zero other violations
# anywhere (docs/data_profile.md). A 5% gate would therefore have been passing
# by well under one percentage point, which is too fragile to trust as a real
# safety check: ordinary variation in a re-extract could trip it. 6% sits
# deliberately above the known floor, so the gate still catches a genuinely
# abnormal batch without firing on data already confirmed fine.
#
# ADR-005 records this as final for this dataset, not provisional — it should
# be revisited only if the pipeline is pointed at a different source file with
# a different noise profile.
REJECTION_RATE_THRESHOLD = 0.06


class QualityGateError(Exception):
    """The batch's rejection rate reached or exceeded the ADR-005 threshold.

    Raised only for that condition, so a caller can distinguish "the data was
    abnormal" from "the pipeline malfunctioned" — the same distinction
    staging.pipeline_runs draws between its 'quality_gate_failed' and 'failed'
    statuses.

    This is a deterministic verdict on a fixed set of counts: re-running the
    task cannot produce a different answer. See the note on retries in
    quality_gate_check.
    """


def quality_gate_check(validation_summary: dict[str, Any]) -> dict[str, Any]:
    """Fail the pipeline if too large a share of the batch was rejected.

    Args:
        validation_summary: the counts-only dict returned by
            validate_and_quarantine, carrying at least "rejection_rate".

    Returns:
        The same summary, unchanged, so downstream tasks can consume it
        without validate_and_quarantine's XCom having to be fetched twice.

    Raises:
        QualityGateError: rejection_rate >= REJECTION_RATE_THRESHOLD. ADR-005
            specifies `>= 6%` as failing, so a rate landing exactly on the
            threshold fails rather than passes.
        TypeError: the summary, or the rate inside it, is the wrong type.
        ValueError: the summary is structurally right but unusable — the key
            is absent, or the rate is outside 0..1.

        The last two are deliberately NOT QualityGateError: a broken summary
        means the upstream task is wrong, which is a different problem from
        the data being abnormal, and reporting it as a quality-gate failure
        would misdirect whoever reads the run. Catch `(TypeError, ValueError)`
        to handle both as "bad input".

    A note on retries: this verdict is deterministic, so an Airflow retry will
    reach the identical conclusion and only delay the failure. The DAG should
    either set retries=0 on this task or convert this into
    AirflowFailException at the task boundary. That conversion is deliberately
    not done here, so this module stays importable and testable without
    Airflow installed.
    """
    rejection_rate = _extract_rejection_rate(validation_summary)

    rejected = validation_summary.get("rejected_row_count")
    staged = validation_summary.get("staged_row_count")
    run_id = validation_summary.get("pipeline_run_id", "unknown")
    observed = f"{rejection_rate:.2%}"
    threshold = f"{REJECTION_RATE_THRESHOLD:.2%}"

    # ADR-005 is explicit that >= threshold fails, so equality is a failure.
    # Written as `>=` rather than `>` for exactly that reason.
    if rejection_rate >= REJECTION_RATE_THRESHOLD:
        message = (
            f"Quality gate failed for run {run_id}: rejection rate {observed} "
            f"reached the {threshold} threshold (ADR-005). "
            f"{_describe_counts(rejected, staged)}"
            "Downstream tasks will not run. This indicates an abnormal batch "
            "rather than a pipeline fault — inspect staging.quarantine grouped "
            "by rule_violated to see which rule is driving the rate."
        )
        logger.error(message)
        raise QualityGateError(message)

    headroom = REJECTION_RATE_THRESHOLD - rejection_rate
    if rejected == 0:
        # Nothing was rejected, so there is nothing to warn about. Logging a
        # warning on a spotless batch would train readers to ignore the
        # warning that matters.
        logger.info(
            "Quality gate passed for run %s: no rows rejected (threshold %s).",
            run_id,
            threshold,
        )
    else:
        # ADR-005's "continue with a warning". Rows were dropped from the
        # serving layer, which is worth surfacing on every run even when the
        # rate is expected — for this dataset it should sit near 4.42%.
        logger.warning(
            "Quality gate passed for run %s: rejection rate %s is below the %s "
            "threshold (%.2f percentage points of headroom). %s"
            "Rejected rows are recorded in staging.quarantine, not dropped.",
            run_id,
            observed,
            threshold,
            headroom * 100,
            _describe_counts(rejected, staged),
        )

    return validation_summary


def _extract_rejection_rate(validation_summary: Any) -> float:
    """Pull rejection_rate out of the summary, failing clearly if it is unusable.

    Guarded rather than indexed directly because this value arrives over XCom
    from another task: a bare KeyError or a TypeError raised deep inside a
    comparison would say far less about what actually went wrong.
    """
    if not isinstance(validation_summary, dict):
        raise TypeError(
            "Validation summary must be a dict as returned by "
            f"validate_and_quarantine, got {type(validation_summary).__name__}."
        )

    if "rejection_rate" not in validation_summary:
        raise ValueError(
            "Validation summary is missing 'rejection_rate'. Keys present: "
            f"{sorted(validation_summary)}."
        )

    rejection_rate = validation_summary["rejection_rate"]

    # bool is a subclass of int, and True would otherwise compare as 1.0 and
    # trip the gate with a nonsense "100%" reading.
    if isinstance(rejection_rate, bool) or not isinstance(
        rejection_rate, (int, float)
    ):
        raise TypeError(
            "Validation summary 'rejection_rate' must be a number, got "
            f"{rejection_rate!r}."
        )

    if not 0.0 <= rejection_rate <= 1.0:
        raise ValueError(
            "Validation summary 'rejection_rate' must be a fraction between 0 "
            f"and 1, got {rejection_rate!r}. (6% is 0.06, not 6.)"
        )

    return float(rejection_rate)


def _describe_counts(rejected: Any, staged: Any) -> str:
    """Render the counts for a log line, tolerating their absence."""
    if rejected is None or staged is None:
        return ""
    return f"{rejected} of {staged} rows rejected. "
