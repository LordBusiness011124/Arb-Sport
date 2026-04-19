"""Data structures for sportsbook events, lines, and normalized probabilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class SportsbookGame:
    """Normalized sportsbook game used for event matching."""

    game_id: str
    league: str
    home_team: str
    away_team: str
    start_time: datetime


@dataclass(slots=True)
class SportsbookMoneylineMarket:
    """Normalized two-outcome sportsbook market with vig removed."""

    game: SportsbookGame
    bookmaker: str
    fetched_at: datetime
    home_american_odds: int
    away_american_odds: int
    home_implied_probability: float
    away_implied_probability: float
    home_fair_probability: float
    away_fair_probability: float
