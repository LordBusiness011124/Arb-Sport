"""Standalone sportsbook math helpers.

This module contains pure functions for validating American odds input,
converting American odds into implied probability, and removing vig from
two-outcome markets. The functions are dependency-free so they are easy to
test and safe to reuse across clients and services.
"""


def validate_odds_input(american_odds: int) -> int:
    """Validate a single American odds value.

    Args:
        american_odds: The sportsbook price expressed as American odds.

    Returns:
        The validated odds value unchanged.

    Raises:
        TypeError: If the odds value is not an integer.
        ValueError: If the odds value is zero, which is invalid in American odds.
    """

    if isinstance(american_odds, bool) or not isinstance(american_odds, int):
        raise TypeError("American odds must be provided as an integer.")

    if american_odds == 0:
        raise ValueError("American odds cannot be zero.")

    return american_odds


def american_odds_to_probability(american_odds: int) -> float:
    """Convert American odds into implied probability.

    Positive odds use the formula ``100 / (odds + 100)``.
    Negative odds use the formula ``abs(odds) / (abs(odds) + 100)``.

    Args:
        american_odds: The sportsbook price expressed as American odds.

    Returns:
        The implied probability as a float between 0 and 1.

    Raises:
        TypeError: If the odds value is not an integer.
        ValueError: If the odds value is zero.
    """

    validated_odds = validate_odds_input(american_odds)

    if validated_odds > 0:
        return 100 / (validated_odds + 100)

    absolute_odds = abs(validated_odds)
    return absolute_odds / (absolute_odds + 100)


def remove_vig_two_outcome(
    outcome_a_probability: float,
    outcome_b_probability: float,
) -> tuple[float, float]:
    """Remove vig from a two-outcome market.

    This normalizes a pair of implied probabilities so they sum to exactly 1.0.

    Args:
        outcome_a_probability: Implied probability for outcome A.
        outcome_b_probability: Implied probability for outcome B.

    Returns:
        A tuple of fair probabilities for outcomes A and B.

    Raises:
        TypeError: If either probability is not a real number.
        ValueError: If either probability is outside ``0 < p < 1`` or if the
            combined probability is not positive.
    """

    for probability in (outcome_a_probability, outcome_b_probability):
        if isinstance(probability, bool) or not isinstance(probability, (int, float)):
            raise TypeError("Probabilities must be numeric.")
        if probability <= 0 or probability >= 1:
            raise ValueError("Probabilities must be greater than 0 and less than 1.")

    total_probability = outcome_a_probability + outcome_b_probability
    if total_probability <= 0:
        raise ValueError("Combined probability must be positive.")

    return (
        outcome_a_probability / total_probability,
        outcome_b_probability / total_probability,
    )
