"""Data structures for detected pricing discrepancies and alert payloads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class Opportunity:
    """A detected edge between sportsbook fair probability and Kalshi price."""

    sportsbook_game_id: str
    event_label: str
    kalshi_ticker: str
    side: str
    team: str
    fair_probability: float
    executable_price: float
    available_size: float
    raw_edge: float
    net_edge: float
    match_confidence: float
    explanation: str
    detected_at: datetime
