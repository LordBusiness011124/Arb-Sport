"""Tests for sportsbook math helpers."""

import math

import pytest

from arb.core.odds import (
    american_odds_to_probability,
    remove_vig_two_outcome,
    validate_odds_input,
)


def test_validate_odds_input_accepts_positive_integer() -> None:
    assert validate_odds_input(150) == 150


def test_validate_odds_input_accepts_negative_integer() -> None:
    assert validate_odds_input(-125) == -125


@pytest.mark.parametrize("invalid_value", [0])
def test_validate_odds_input_rejects_zero(invalid_value: int) -> None:
    with pytest.raises(ValueError, match="cannot be zero"):
        validate_odds_input(invalid_value)


@pytest.mark.parametrize("invalid_value", [1.5, "100", None, True, False])
def test_validate_odds_input_rejects_non_integer_values(invalid_value: object) -> None:
    with pytest.raises(TypeError, match="must be provided as an integer"):
        validate_odds_input(invalid_value)  # type: ignore[arg-type]


def test_american_odds_to_probability_handles_positive_odds() -> None:
    assert american_odds_to_probability(150) == pytest.approx(0.4)


def test_american_odds_to_probability_handles_negative_odds() -> None:
    assert american_odds_to_probability(-150) == pytest.approx(0.6)


def test_american_odds_to_probability_handles_even_money() -> None:
    assert american_odds_to_probability(100) == pytest.approx(0.5)
    assert american_odds_to_probability(-100) == pytest.approx(0.5)


def test_american_odds_to_probability_handles_large_positive_odds() -> None:
    assert american_odds_to_probability(10000) == pytest.approx(100 / 10100)


def test_american_odds_to_probability_raises_for_zero() -> None:
    with pytest.raises(ValueError, match="cannot be zero"):
        american_odds_to_probability(0)


@pytest.mark.parametrize("invalid_value", [2.5, "200", None, True])
def test_american_odds_to_probability_rejects_invalid_type(invalid_value: object) -> None:
    with pytest.raises(TypeError, match="must be provided as an integer"):
        american_odds_to_probability(invalid_value)  # type: ignore[arg-type]


def test_remove_vig_two_outcome_normalizes_probabilities() -> None:
    fair_a, fair_b = remove_vig_two_outcome(0.5238, 0.5652)

    assert fair_a == pytest.approx(0.4810, abs=1e-4)
    assert fair_b == pytest.approx(0.5190, abs=1e-4)
    assert fair_a + fair_b == pytest.approx(1.0)


def test_remove_vig_two_outcome_handles_balanced_market() -> None:
    fair_a, fair_b = remove_vig_two_outcome(0.5, 0.5)

    assert fair_a == pytest.approx(0.5)
    assert fair_b == pytest.approx(0.5)


@pytest.mark.parametrize("invalid_probability", [0, 1, -0.1, 1.2])
def test_remove_vig_two_outcome_rejects_out_of_range_probabilities(
    invalid_probability: float,
) -> None:
    with pytest.raises(ValueError, match="greater than 0 and less than 1"):
        remove_vig_two_outcome(invalid_probability, 0.5)


@pytest.mark.parametrize("invalid_probability", ["0.5", None, True, False])
def test_remove_vig_two_outcome_rejects_non_numeric_probabilities(
    invalid_probability: object,
) -> None:
    with pytest.raises(TypeError, match="must be numeric"):
        remove_vig_two_outcome(invalid_probability, 0.5)  # type: ignore[arg-type]


def test_remove_vig_two_outcome_result_stays_between_zero_and_one() -> None:
    fair_a, fair_b = remove_vig_two_outcome(0.01, 0.99)

    assert 0 < fair_a < 1
    assert 0 < fair_b < 1
    assert math.isclose(fair_a + fair_b, 1.0)
