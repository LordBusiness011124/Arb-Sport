"""Tests for conservative sportsbook-to-Kalshi event matching."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from arb.core.matching import match_sportsbook_game_to_kalshi_market, normalize_team_name
from arb.models.market import KalshiEventMarket
from arb.models.odds import SportsbookGame


def build_sportsbook_game(
    home_team: str = "North Carolina",
    away_team: str = "Duke",
    league: str = "NCAAB",
    start_time: datetime | None = None,
) -> SportsbookGame:
    """Build a reusable sportsbook game fixture."""

    return SportsbookGame(
        game_id="game-1",
        league=league,
        home_team=home_team,
        away_team=away_team,
        start_time=start_time or datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
    )


def build_kalshi_market(
    team_a: str = "UNC",
    team_b: str = "Duke",
    league: str = "college basketball",
    start_time: datetime | None = None,
) -> KalshiEventMarket:
    """Build a reusable Kalshi event fixture."""

    return KalshiEventMarket(
        ticker="KXTEST-1",
        league=league,
        team_a=team_a,
        team_b=team_b,
        start_time=start_time or datetime(2026, 4, 20, 1, 30, tzinfo=UTC),
        title="UNC vs Duke",
    )


def test_normalize_team_name_handles_known_abbreviation() -> None:
    assert normalize_team_name("UNC") == "north carolina"


def test_match_accepts_safe_team_alias_and_time_proximity() -> None:
    result = match_sportsbook_game_to_kalshi_market(
        build_sportsbook_game(),
        build_kalshi_market(),
    )

    assert result.match is True
    assert result.confidence_score >= 0.9
    assert "Matched on league" in result.explanation


def test_match_accepts_swapped_team_order() -> None:
    result = match_sportsbook_game_to_kalshi_market(
        build_sportsbook_game(home_team="Duke", away_team="North Carolina"),
        build_kalshi_market(team_a="UNC", team_b="Duke"),
    )

    assert result.match is True


def test_match_rejects_partial_team_overlap() -> None:
    result = match_sportsbook_game_to_kalshi_market(
        build_sportsbook_game(home_team="North Carolina", away_team="Duke"),
        build_kalshi_market(team_a="North Carolina", team_b="Wake Forest"),
    )

    assert result.match is False
    assert result.confidence_score == 0.0
    assert "both teams did not match exactly" in result.explanation


def test_match_rejects_ambiguous_abbreviation() -> None:
    result = match_sportsbook_game_to_kalshi_market(
        build_sportsbook_game(home_team="USC", away_team="UCLA"),
        build_kalshi_market(team_a="Southern California", team_b="UCLA"),
    )

    assert result.match is False
    assert "ambiguous team name" in result.explanation


def test_match_rejects_wrong_league() -> None:
    result = match_sportsbook_game_to_kalshi_market(
        build_sportsbook_game(league="NBA"),
        build_kalshi_market(league="college basketball"),
    )

    assert result.match is False
    assert result.confidence_score == 0.0
    assert "leagues do not match" in result.explanation


def test_match_rejects_start_times_that_are_too_far_apart() -> None:
    result = match_sportsbook_game_to_kalshi_market(
        build_sportsbook_game(),
        build_kalshi_market(start_time=datetime(2026, 4, 20, 6, 0, tzinfo=UTC)),
        max_start_time_diff_minutes=120,
    )

    assert result.match is False
    assert "start times differ" in result.explanation


def test_match_rejects_when_confidence_is_below_threshold() -> None:
    sportsbook_start = datetime(2026, 4, 20, 1, 0, tzinfo=UTC)
    kalshi_start = sportsbook_start + timedelta(minutes=90)

    result = match_sportsbook_game_to_kalshi_market(
        build_sportsbook_game(start_time=sportsbook_start),
        build_kalshi_market(start_time=kalshi_start),
        max_start_time_diff_minutes=120,
        min_confidence_score=0.95,
    )

    assert result.match is False
    assert result.confidence_score < 0.95
    assert "below threshold" in result.explanation


def test_match_rejects_naive_sportsbook_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        match_sportsbook_game_to_kalshi_market(
            build_sportsbook_game(start_time=datetime(2026, 4, 20, 1, 0)),
            build_kalshi_market(),
        )


def test_match_rejects_naive_kalshi_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        match_sportsbook_game_to_kalshi_market(
            build_sportsbook_game(),
            build_kalshi_market(start_time=datetime(2026, 4, 20, 1, 30)),
        )


@pytest.mark.parametrize(
    ("max_minutes", "min_confidence"),
    [(-1, 0.9), (120, 1.1)],
)
def test_match_rejects_invalid_configuration(
    max_minutes: int,
    min_confidence: float,
) -> None:
    with pytest.raises(ValueError):
        match_sportsbook_game_to_kalshi_market(
            build_sportsbook_game(),
            build_kalshi_market(),
            max_start_time_diff_minutes=max_minutes,
            min_confidence_score=min_confidence,
        )
