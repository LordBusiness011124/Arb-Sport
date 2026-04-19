"""Data structures for normalized Kalshi market data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class KalshiOrderLevel:
    """A single order-book level normalized into floats."""

    price: float
    size: float


@dataclass(slots=True)
class KalshiMarketSnapshot:
    """Normalized view of a Kalshi binary market and its best executable prices."""

    ticker: str
    event_ticker: str
    event_title: str
    event_sub_title: str
    market_title: str
    series_ticker: str
    category: str
    status: str
    fetched_at: datetime
    occurrence_time: datetime | None
    market_type: str
    yes_sub_title: str
    no_sub_title: str
    rules_primary: str
    yes_ask_levels: tuple[KalshiOrderLevel, ...]
    no_ask_levels: tuple[KalshiOrderLevel, ...]
    yes_bid: float | None
    yes_bid_size: float | None
    yes_ask: float | None
    yes_ask_size: float | None
    no_bid: float | None
    no_bid_size: float | None
    no_ask: float | None
    no_ask_size: float | None


@dataclass(slots=True)
class KalshiEventMarket:
    """Normalized Kalshi sports event used for conservative game matching."""

    ticker: str
    league: str
    team_a: str
    team_b: str
    start_time: datetime
    title: str = ""
