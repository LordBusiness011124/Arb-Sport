"""Conservative event matching for sportsbook games and Kalshi markets.

Design goals:
1. Favor false negatives over false positives.
2. Require exact league agreement after normalization.
3. Require both teams to match after cautious team-name normalization.
4. Require start times to be sufficiently close.
5. Reject ambiguous abbreviations instead of guessing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from arb.models.market import KalshiEventMarket
from arb.models.odds import SportsbookGame

TEAM_ALIAS_MAP = {
    "unc": "north carolina",
    "north carolina tar heels": "north carolina",
    "tar heels": "north carolina",
    "uconn": "connecticut",
    "uconn huskies": "connecticut",
    "ole miss": "mississippi",
    "mississippi rebels": "mississippi",
    "ucf": "central florida",
    "unlv": "nevada las vegas",
    "byu": "brigham young",
    "smu": "southern methodist",
    "lsu": "louisiana state",
    "tcu": "texas christian",
    "utsa": "texas san antonio",
    "utep": "texas el paso",
    "umass": "massachusetts",
}

AMBIGUOUS_ABBREVIATIONS = {
    "usc",
    "miami",
}

LEAGUE_ALIAS_MAP = {
    "ncaaf": "college football",
    "cfb": "college football",
    "ncaa football": "college football",
    "ncaab": "college basketball",
    "cbb": "college basketball",
    "ncaa basketball": "college basketball",
    "mlb": "mlb",
    "nba": "nba",
    "wnba": "wnba",
    "nfl": "nfl",
    "nhl": "nhl",
}


@dataclass(slots=True)
class MatchResult:
    """Result of comparing a sportsbook game against one Kalshi event."""

    match: bool
    confidence_score: float
    explanation: str


def normalize_team_name(team_name: str) -> str:
    """Normalize a team name into a conservative canonical form.

    Ambiguous short names are preserved rather than expanded so matching can
    reject them safely.
    """

    normalized = _normalize_text(team_name)
    return TEAM_ALIAS_MAP.get(normalized, normalized)


def match_sportsbook_game_to_kalshi_market(
    sportsbook_game: SportsbookGame,
    kalshi_market: KalshiEventMarket,
    max_start_time_diff_minutes: int = 120,
    min_confidence_score: float = 0.9,
) -> MatchResult:
    """Conservatively decide whether a sportsbook game matches a Kalshi market.

    Confidence is built from three signals:
    - league agreement: required
    - both team names agreeing after normalization: required
    - start times being close: required

    The function rejects ambiguous team abbreviations and partial team overlap.
    """

    if max_start_time_diff_minutes < 0:
        raise ValueError("Maximum start time difference cannot be negative.")
    if not 0 <= min_confidence_score <= 1:
        raise ValueError("Minimum confidence score must be between 0 and 1.")
    if not _is_timezone_aware(sportsbook_game.start_time):
        raise ValueError("Sportsbook start time must be timezone-aware.")
    if not _is_timezone_aware(kalshi_market.start_time):
        raise ValueError("Kalshi start time must be timezone-aware.")

    sportsbook_league = _normalize_league(sportsbook_game.league)
    kalshi_league = _normalize_league(kalshi_market.league)
    if sportsbook_league != kalshi_league:
        return MatchResult(False, 0.0, "Rejected: leagues do not match.")

    sportsbook_teams = [sportsbook_game.home_team, sportsbook_game.away_team]
    kalshi_teams = [kalshi_market.team_a, kalshi_market.team_b]

    ambiguous = _find_ambiguous_team_names(sportsbook_teams + kalshi_teams)
    if ambiguous:
        names = ", ".join(sorted(ambiguous))
        return MatchResult(False, 0.0, f"Rejected: ambiguous team name(s): {names}.")

    normalized_sportsbook_teams = {normalize_team_name(team) for team in sportsbook_teams}
    normalized_kalshi_teams = {normalize_team_name(team) for team in kalshi_teams}
    if normalized_sportsbook_teams != normalized_kalshi_teams:
        return MatchResult(
            False,
            0.0,
            "Rejected: both teams did not match exactly after normalization.",
        )

    start_diff_minutes = abs(
        (sportsbook_game.start_time - kalshi_market.start_time).total_seconds()
    ) / 60.0
    if start_diff_minutes > max_start_time_diff_minutes:
        return MatchResult(
            False,
            0.0,
            f"Rejected: start times differ by {start_diff_minutes:.0f} minutes.",
        )

    confidence_score = _calculate_confidence(start_diff_minutes, max_start_time_diff_minutes)
    if confidence_score < min_confidence_score:
        return MatchResult(
            False,
            confidence_score,
            f"Rejected: confidence {confidence_score:.2f} is below threshold.",
        )

    return MatchResult(
        True,
        confidence_score,
        (
            "Matched on league, both teams, and start time proximity "
            f"({start_diff_minutes:.0f} minutes apart)."
        ),
    )


def _normalize_text(value: str) -> str:
    """Normalize user-facing text for conservative comparison."""

    lowered = value.lower().strip()
    lowered = lowered.replace("&", " and ")
    lowered = re.sub(r"[^\w\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def _normalize_league(league: str) -> str:
    """Normalize league names and common abbreviations."""

    normalized = _normalize_text(league)
    return LEAGUE_ALIAS_MAP.get(normalized, normalized)


def _find_ambiguous_team_names(team_names: list[str]) -> set[str]:
    """Return team names that are too ambiguous to match safely."""

    ambiguous: set[str] = set()
    for name in team_names:
        normalized = _normalize_text(name)
        if normalized in AMBIGUOUS_ABBREVIATIONS:
            ambiguous.add(name)
    return ambiguous


def _calculate_confidence(
    start_diff_minutes: float,
    max_start_time_diff_minutes: int,
) -> float:
    """Convert time proximity into a conservative confidence score."""

    if max_start_time_diff_minutes == 0:
        return 1.0 if start_diff_minutes == 0 else 0.0

    time_score = 1.0 - (start_diff_minutes / max_start_time_diff_minutes)
    return round(0.9 + (max(time_score, 0.0) * 0.1), 4)


def _is_timezone_aware(value: datetime) -> bool:
    """Return whether a datetime has timezone information attached."""

    return value.tzinfo is not None and value.utcoffset() is not None
