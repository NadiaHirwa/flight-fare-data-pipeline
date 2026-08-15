"""Unit tests for the ADR-005 data-quality gate.

No database and no Airflow — the gate is a pure decision over the counts-only
summary validate_and_quarantine returns, and these tests depend on that.

The boundary cases carry the weight here. ADR-005 specifies `< 6%` continues
and `>= 6%` fails, so a rate landing exactly on the threshold must fail; that
is the single most likely thing to be implemented backwards, and it is the
case a real abnormal batch is most likely to land on.
"""

import logging

import pytest

from src.quality.gate import (
    REJECTION_RATE_THRESHOLD,
    QualityGateError,
    quality_gate_check,
)

# The real figure from Phase 0: 2,522 of 57,000 rows fail the Total Fare
# reconciliation rule, with zero other violations (docs/data_profile.md).
PHASE_0_REJECTED = 2522
PHASE_0_STAGED = 57000
PHASE_0_RATE = round(PHASE_0_REJECTED / PHASE_0_STAGED, 6)  # 0.044246


def summary(rejection_rate, rejected=None, staged=1000, run_id="manual__test"):
    """A validation summary shaped like validate_and_quarantine's return."""
    if rejected is None:
        rejected = round(rejection_rate * staged)
    return {
        "pipeline_run_id": run_id,
        "staged_row_count": staged,
        "valid_row_count": staged - rejected,
        "rejected_row_count": rejected,
        "violation_count": rejected,
        "rejection_rate": rejection_rate,
    }


# ---------------------------------------------------------------------------
# The threshold itself
# ---------------------------------------------------------------------------

def test_threshold_is_six_percent():
    """ADR-005, finalized after Phase 0 — not the original 5% placeholder."""
    assert REJECTION_RATE_THRESHOLD == 0.06


def test_threshold_sits_above_the_known_noise_floor():
    """ADR-005's actual reasoning: 6% is above the measured 4.42% so the gate
    is not tripped by data already confirmed fine."""
    assert PHASE_0_RATE < REJECTION_RATE_THRESHOLD


# ---------------------------------------------------------------------------
# Comfortably under
# ---------------------------------------------------------------------------

def test_comfortably_under_threshold_passes():
    result = quality_gate_check(summary(0.01))
    assert result["rejection_rate"] == 0.01


def test_zero_rejections_passes():
    assert quality_gate_check(summary(0.0, rejected=0)) is not None


@pytest.mark.parametrize("rate", [0.0, 0.005, 0.01, 0.025, 0.0442, 0.05, 0.0599])
def test_rates_below_threshold_all_pass(rate):
    quality_gate_check(summary(rate))


# ---------------------------------------------------------------------------
# The Phase 0 figure
# ---------------------------------------------------------------------------

def test_real_phase_0_rate_passes_the_gate():
    """4.42% — the dataset's natural noise floor — must not trip the gate.

    This is the case ADR-005 was rewritten for: under the original 5%
    placeholder this passed by well under one percentage point.
    """
    result = quality_gate_check(
        summary(PHASE_0_RATE, rejected=PHASE_0_REJECTED, staged=PHASE_0_STAGED)
    )
    assert result["rejected_row_count"] == 2522


def test_real_phase_0_rate_would_have_been_marginal_under_a_five_percent_gate():
    """Documents why the threshold moved, so the reasoning is executable and
    not only prose in an ADR."""
    assert PHASE_0_RATE < 0.05
    assert 0.05 - PHASE_0_RATE < 0.01  # margin under one percentage point
    assert REJECTION_RATE_THRESHOLD - PHASE_0_RATE > 0.01  # comfortable now


# ---------------------------------------------------------------------------
# Exactly at threshold — must FAIL, per ADR-005's ">= 6%"
# ---------------------------------------------------------------------------

def test_exactly_at_threshold_fails():
    """ADR-005 says `>= 6%` fails, so the boundary itself is a failure."""
    with pytest.raises(QualityGateError):
        quality_gate_check(summary(0.06))


@pytest.mark.parametrize(
    ("rejected", "staged"),
    [(6, 100), (3, 50), (60, 1000), (600, 10_000), (3420, 57_000)],
)
def test_exact_six_percent_from_real_counts_fails(rejected, staged):
    """Computed the way validate_and_quarantine computes it, so this covers the
    float representation of 6% rather than only the literal 0.06."""
    rate = round(rejected / staged, 6)
    with pytest.raises(QualityGateError):
        quality_gate_check(summary(rate, rejected=rejected, staged=staged))


# ---------------------------------------------------------------------------
# Just over
# ---------------------------------------------------------------------------

def test_just_over_threshold_fails():
    with pytest.raises(QualityGateError):
        quality_gate_check(summary(0.060001))


@pytest.mark.parametrize("rate", [0.0601, 0.07, 0.10, 0.5, 1.0])
def test_rates_above_threshold_all_fail(rate):
    with pytest.raises(QualityGateError):
        quality_gate_check(summary(rate))


def test_just_under_threshold_passes():
    """The other side of the boundary — guards against an off-by-one that
    would reject batches the ADR intends to accept."""
    quality_gate_check(summary(0.059999))


# ---------------------------------------------------------------------------
# Behaviour on pass and on failure
# ---------------------------------------------------------------------------

def test_summary_is_returned_unchanged():
    original = summary(0.02)
    result = quality_gate_check(original)
    assert result == original
    assert result is original


def test_failure_message_names_rate_threshold_and_run():
    with pytest.raises(QualityGateError) as excinfo:
        quality_gate_check(summary(0.08, run_id="manual__bad_batch"))
    message = str(excinfo.value)
    assert "8.00%" in message
    assert "6.00%" in message
    assert "manual__bad_batch" in message
    assert "quarantine" in message  # points at where to investigate


def test_failure_is_distinguishable_from_a_pipeline_fault():
    """staging.pipeline_runs draws the same distinction with its
    'quality_gate_failed' vs 'failed' statuses."""
    with pytest.raises(QualityGateError):
        quality_gate_check(summary(0.09))


def test_passing_with_rejections_logs_a_warning(caplog):
    """ADR-005: '< 6% -> continue with a warning'."""
    with caplog.at_level(logging.DEBUG, logger="src.quality.gate"):
        quality_gate_check(summary(PHASE_0_RATE, rejected=2522, staged=57000))
    assert [r.levelno for r in caplog.records] == [logging.WARNING]
    assert "4.42%" in caplog.text


def test_passing_with_no_rejections_logs_info_not_warning(caplog):
    """A spotless batch has nothing to warn about; warning on it would train
    readers to ignore the warning that matters."""
    with caplog.at_level(logging.DEBUG, logger="src.quality.gate"):
        quality_gate_check(summary(0.0, rejected=0))
    assert [r.levelno for r in caplog.records] == [logging.INFO]


def test_failure_logs_an_error(caplog):
    with (
        caplog.at_level(logging.DEBUG, logger="src.quality.gate"),
        pytest.raises(QualityGateError),
    ):
        quality_gate_check(summary(0.07))
    assert [r.levelno for r in caplog.records] == [logging.ERROR]


# ---------------------------------------------------------------------------
# Malformed input — a broken upstream task, not an abnormal batch
# ---------------------------------------------------------------------------

def test_missing_rejection_rate_raises_value_error_not_gate_error():
    incomplete = summary(0.01)
    del incomplete["rejection_rate"]
    with pytest.raises(ValueError) as excinfo:
        quality_gate_check(incomplete)
    assert not isinstance(excinfo.value, QualityGateError)
    assert "rejection_rate" in str(excinfo.value)


@pytest.mark.parametrize("bad", [None, "0.04", [], 42])
def test_non_dict_summary_raises_type_error(bad):
    """Wrong type, so TypeError rather than ValueError."""
    with pytest.raises(TypeError):
        quality_gate_check(bad)


@pytest.mark.parametrize("bad", ["0.04", None, True, False])
def test_non_numeric_rejection_rate_raises_type_error(bad):
    """True would otherwise compare as 1.0 and trip the gate as '100%'."""
    with pytest.raises(TypeError):
        quality_gate_check(summary(0.01) | {"rejection_rate": bad})


@pytest.mark.parametrize("bad", [None, "0.04", [], 42, True])
def test_bad_input_is_never_reported_as_a_quality_gate_failure(bad):
    """A broken upstream task must not be logged as an abnormal batch."""
    with pytest.raises((TypeError, ValueError)) as excinfo:
        quality_gate_check(bad)
    assert not isinstance(excinfo.value, QualityGateError)


@pytest.mark.parametrize("bad", [-0.01, 1.5, 6])
def test_out_of_range_rejection_rate_raises_value_error(bad):
    """6 is the classic mistake: 6% is 0.06, not 6."""
    with pytest.raises(ValueError):
        quality_gate_check(summary(0.01) | {"rejection_rate": bad})


def test_gate_works_without_the_optional_count_fields():
    """Only rejection_rate is required; the counts are used for messaging."""
    quality_gate_check({"rejection_rate": 0.01})
    with pytest.raises(QualityGateError):
        quality_gate_check({"rejection_rate": 0.5})
