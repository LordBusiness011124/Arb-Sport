"""Sportsbook odds client using The Odds API v4."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx

from arb.core.odds import american_odds_to_probability, remove_vig_two_outcome
from arb.models.odds import SportsbookGame, SportsbookMoneylineMarket


class SportsbookAPIError(RuntimeError):
    """Raised when sportsbook odds cannot be fetched or normalized."""


class SportsbookClient:
    """Thin client for fetching two-outcome moneyline odds from The Odds API."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        bookmaker: str,
        sport: str,
        regions: str = "us",
        timeout: float = 15.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._bookmaker = bookmaker
        self._sport = sport
        self._regions = regions
        self._timeout = timeout

    def fetch_moneyline_markets(self) -> list[SportsbookMoneylineMarket]:
        """Fetch and normalize current moneyline markets for one sport/bookmaker."""

        fetched_at = datetime.now(tz=UTC)

        url = f"{self._base_url}/v4/sports/{self._sport}/odds"
        params = {
            "apiKey": self._api_key,
            "regions": self._regions,
            "markets": "h2h",
            "oddsFormat": "american",
            "dateFormat": "iso",
            "bookmakers": self._bookmaker,
        }

        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, params=params)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise SportsbookAPIError(f"Sportsbook request failed: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise SportsbookAPIError("Sportsbook returned non-JSON data.") from exc

        if not isinstance(payload, list):
            raise SportsbookAPIError("Sportsbook returned an unexpected payload.")

        markets: list[SportsbookMoneylineMarket] = []
        for event in payload:
            normalized = self._normalize_event(event, fetched_at=fetched_at)
            if normalized is not None:
                markets.append(normalized)
        return markets

    def _normalize_event(
        self,
        event: object,
        fetched_at: datetime,
    ) -> SportsbookMoneylineMarket | None:
        """Normalize one odds event into a two-outcome market."""

        if not isinstance(event, dict):
            return None

        bookmakers = event.get("bookmakers")
        if not isinstance(bookmakers, list) or not bookmakers:
            return None

        bookmaker = bookmakers[0]
        if not isinstance(bookmaker, dict):
            return None

        markets = bookmaker.get("markets")
        if not isinstance(markets, list):
            return None

        moneyline_market = next(
            (market for market in markets if isinstance(market, dict) and market.get("key") == "h2h"),
            None,
        )
        if not isinstance(moneyline_market, dict):
            return None

        outcomes = moneyline_market.get("outcomes")
        if not isinstance(outcomes, list) or len(outcomes) != 2:
            return None

        home_team = str(event.get("home_team", "")).strip()
        away_team = str(event.get("away_team", "")).strip()
        event_id = str(event.get("id", "")).strip()
        commence_time = str(event.get("commence_time", "")).strip()

        if not all([home_team, away_team, event_id, commence_time]):
            return None

        start_time = _parse_required_datetime(commence_time)
        if start_time is None:
            return None
        outcome_map: dict[str, int] = {}
        for outcome in outcomes:
            if not isinstance(outcome, dict):
                continue
            name = str(outcome.get("name", "")).strip()
            price = outcome.get("price")
            if not isinstance(price, int):
                continue
            outcome_map[name] = price

        if home_team not in outcome_map or away_team not in outcome_map:
            return None

        home_odds = outcome_map[home_team]
        away_odds = outcome_map[away_team]
        home_implied = american_odds_to_probability(home_odds)
        away_implied = american_odds_to_probability(away_odds)
        home_fair, away_fair = remove_vig_two_outcome(home_implied, away_implied)

        return SportsbookMoneylineMarket(
            game=SportsbookGame(
                game_id=event_id,
                league=self._sport,
                home_team=home_team,
                away_team=away_team,
                start_time=start_time,
            ),
            bookmaker=str(bookmaker.get("key", self._bookmaker)),
            fetched_at=fetched_at,
            home_american_odds=home_odds,
            away_american_odds=away_odds,
            home_implied_probability=home_implied,
            away_implied_probability=away_implied,
            home_fair_probability=home_fair,
            away_fair_probability=away_fair,
        )


def _parse_required_datetime(value: str) -> datetime | None:
    """Parse a required ISO 8601 datetime and reject naive values."""

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None

    return parsed.astimezone(UTC)
