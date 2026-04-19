"""Kalshi market-data client.

This module fetches active sports markets and their order books from Kalshi's
public market-data API. The client normalizes the result into a compact
internal format that exposes best bid, best ask, and available size for both
YES and NO sides of each binary market.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from datetime import UTC, datetime

import httpx

from arb.models.market import KalshiMarketSnapshot, KalshiOrderLevel

DEFAULT_KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
MAX_KALSHI_EVENTS_PAGE_SIZE = 200
MAX_REJECTION_SAMPLES = 5

logger = logging.getLogger(__name__)


class KalshiAPIError(RuntimeError):
    """Raised when Kalshi market data cannot be fetched or parsed."""


class KalshiClient:
    """Thin HTTP client for Kalshi public market data."""

    def __init__(self, base_url: str = DEFAULT_KALSHI_BASE_URL, timeout: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def fetch_active_sports_markets(self, limit: int = 25) -> list[KalshiMarketSnapshot]:
        """Fetch open sports markets and enrich them with best order-book prices.

        Args:
            limit: Maximum number of normalized market snapshots to return.

        Returns:
            A list of normalized market snapshots. Markets whose order books fail
            to load are skipped so a single bad market does not fail the batch.

        Raises:
            KalshiAPIError: If the initial market/event query fails.
        """

        query_limit = max(limit, 1)
        fetched_at = datetime.now(tz=UTC)
        params = {
            "status": "open",
            "with_nested_markets": "true",
            "limit": str(min(max(query_limit * 10, 100), MAX_KALSHI_EVENTS_PAGE_SIZE)),
        }

        snapshots: list[KalshiMarketSnapshot] = []
        cursor = ""

        while len(snapshots) < query_limit:
            if cursor:
                params["cursor"] = cursor
            elif "cursor" in params:
                del params["cursor"]

            payload = self._get_json("/events", params=params)
            events = payload.get("events")
            if not isinstance(events, list):
                raise KalshiAPIError("Kalshi response did not include a valid events list.")

            for event in events:
                if not isinstance(event, dict):
                    continue
                if str(event.get("category", "")).lower() != "sports":
                    continue

                markets = event.get("markets") or []
                for market in markets:
                    if not isinstance(market, dict):
                        continue
                    if str(market.get("status", "")).lower() not in {"open", "active"}:
                        continue

                    ticker = str(market.get("ticker", "")).strip()
                    if not ticker:
                        continue

                    try:
                        orderbook = self.fetch_orderbook(ticker)
                    except KalshiAPIError:
                        continue

                    snapshots.append(
                        KalshiMarketSnapshot(
                            ticker=ticker,
                            event_ticker=str(event.get("event_ticker", "")),
                            event_title=str(event.get("title", "")),
                            event_sub_title=str(event.get("sub_title", "")),
                            market_title=str(market.get("title", "")),
                            series_ticker=str(event.get("series_ticker", "")),
                            category=str(event.get("category", "")),
                            status=str(market.get("status", "")),
                            fetched_at=fetched_at,
                            occurrence_time=_parse_optional_datetime(
                                market.get("occurrence_datetime")
                            ),
                            market_type=str(market.get("market_type", "")),
                            yes_sub_title=str(market.get("yes_sub_title", "")),
                            no_sub_title=str(market.get("no_sub_title", "")),
                            rules_primary=str(market.get("rules_primary", "")),
                            yes_ask_levels=_merge_explicit_best_ask_level(
                                _parse_optional_decimal(market.get("yes_ask_dollars")),
                                _parse_optional_decimal(market.get("yes_ask_size_fp")),
                                orderbook["yes_ask_levels"],
                            ),
                            no_ask_levels=_merge_explicit_best_ask_level(
                                _parse_optional_decimal(market.get("no_ask_dollars")),
                                _parse_optional_decimal(market.get("no_ask_size_fp")),
                                orderbook["no_ask_levels"],
                            ),
                            yes_bid=orderbook["yes_bid"],
                            yes_bid_size=orderbook["yes_bid_size"],
                            yes_ask=_parse_optional_decimal(market.get("yes_ask_dollars"))
                            or orderbook["yes_ask"],
                            yes_ask_size=_parse_optional_decimal(market.get("yes_ask_size_fp"))
                            or orderbook["yes_ask_size"],
                            no_bid=orderbook["no_bid"],
                            no_bid_size=orderbook["no_bid_size"],
                            no_ask=_parse_optional_decimal(market.get("no_ask_dollars"))
                            or orderbook["no_ask"],
                            no_ask_size=_parse_optional_decimal(market.get("no_ask_size_fp"))
                            or orderbook["no_ask_size"],
                        )
                    )

                    if len(snapshots) >= query_limit:
                        return snapshots

            cursor = str(payload.get("cursor", "")).strip()
            if not cursor:
                break

        return snapshots

    def fetch_active_sports_markets_for_series(
        self,
        series_prefixes: tuple[str, ...],
        limit: int = 25,
    ) -> list[KalshiMarketSnapshot]:
        """Fetch open sports markets restricted to specific Kalshi series prefixes."""

        if not series_prefixes:
            return self.fetch_active_sports_markets(limit=limit)

        query_limit = max(limit, 1)
        fetched_at = datetime.now(tz=UTC)
        params = {
            "status": "open",
            "with_nested_markets": "true",
            "limit": str(min(max(query_limit * 10, 100), MAX_KALSHI_EVENTS_PAGE_SIZE)),
        }

        snapshots: list[KalshiMarketSnapshot] = []
        rejection_counts: Counter[str] = Counter()
        rejection_samples: list[str] = []
        cursor = ""

        while len(snapshots) < query_limit:
            if cursor:
                params["cursor"] = cursor
            elif "cursor" in params:
                del params["cursor"]

            payload = self._get_json("/events", params=params)
            events = payload.get("events")
            if not isinstance(events, list):
                raise KalshiAPIError("Kalshi response did not include a valid events list.")

            for event in events:
                if not isinstance(event, dict):
                    continue
                if str(event.get("category", "")).lower() != "sports":
                    continue
                series_ticker = str(event.get("series_ticker", ""))
                if not any(series_ticker.startswith(prefix) for prefix in series_prefixes):
                    continue

                snapshots.extend(
                    self._normalize_event_markets(
                        event=event,
                        fetched_at=fetched_at,
                        remaining=query_limit - len(snapshots),
                        rejection_counts=rejection_counts,
                        rejection_samples=rejection_samples,
                    )
                )
                if len(snapshots) >= query_limit:
                    return snapshots

            cursor = str(payload.get("cursor", "")).strip()
            if not cursor:
                break

        if not snapshots and rejection_counts:
            logger.info(
                "Kalshi prefilter rejected all candidate markets for series=%s. Reasons=%s. Samples=%s",
                ",".join(series_prefixes),
                dict(rejection_counts.most_common()),
                rejection_samples,
            )

        return snapshots

    def fetch_orderbook(self, ticker: str) -> dict[str, float | None]:
        """Fetch and normalize a single market order book.

        Kalshi returns YES bids and NO bids only. In a binary market, the best
        ask on one side is implied by the best bid on the opposite side.

        Args:
            ticker: Market ticker.

        Returns:
            A normalized dictionary containing best bid, best ask, and available
            size for both YES and NO sides.

        Raises:
            KalshiAPIError: If the orderbook request fails or the payload is malformed.
        """

        payload = self._get_json(f"/markets/{ticker}/orderbook")
        orderbook = payload.get("orderbook_fp")
        if not isinstance(orderbook, dict):
            raise KalshiAPIError(f"Kalshi orderbook for {ticker} was missing orderbook_fp.")

        yes_levels = self._parse_levels(orderbook.get("yes_dollars"))
        no_levels = self._parse_levels(orderbook.get("no_dollars"))

        best_yes_bid = yes_levels[0] if yes_levels else None
        best_no_bid = no_levels[0] if no_levels else None

        yes_ask_levels = self._derive_ask_levels(no_levels)
        no_ask_levels = self._derive_ask_levels(yes_levels)
        best_yes_ask = yes_ask_levels[0] if yes_ask_levels else None
        best_no_ask = no_ask_levels[0] if no_ask_levels else None

        return {
            "yes_bid": best_yes_bid.price if best_yes_bid else None,
            "yes_bid_size": best_yes_bid.size if best_yes_bid else None,
            "yes_ask": best_yes_ask.price if best_yes_ask else None,
            "yes_ask_size": best_yes_ask.size if best_yes_ask else None,
            "yes_ask_levels": tuple(yes_ask_levels),
            "no_bid": best_no_bid.price if best_no_bid else None,
            "no_bid_size": best_no_bid.size if best_no_bid else None,
            "no_ask": best_no_ask.price if best_no_ask else None,
            "no_ask_size": best_no_ask.size if best_no_ask else None,
            "no_ask_levels": tuple(no_ask_levels),
        }

    def _normalize_event_markets(
        self,
        event: dict,
        fetched_at: datetime,
        remaining: int,
        rejection_counts: Counter[str] | None = None,
        rejection_samples: list[str] | None = None,
    ) -> list[KalshiMarketSnapshot]:
        """Normalize nested market payloads for one Kalshi event."""

        snapshots: list[KalshiMarketSnapshot] = []
        markets = event.get("markets") or []
        for market in markets:
            if remaining <= 0:
                break
            if not isinstance(market, dict):
                continue
            if str(market.get("status", "")).lower() not in {"open", "active"}:
                continue
            rejection_reason = _matchable_market_rejection_reason(event, market)
            if rejection_reason is not None:
                if rejection_counts is not None:
                    rejection_counts[rejection_reason] += 1
                if rejection_samples is not None and len(rejection_samples) < MAX_REJECTION_SAMPLES:
                    rejection_samples.append(
                        _format_market_rejection_sample(event, market, rejection_reason)
                    )
                continue

            ticker = str(market.get("ticker", "")).strip()
            if not ticker:
                continue

            try:
                orderbook = self.fetch_orderbook(ticker)
            except KalshiAPIError:
                continue

            snapshots.append(
                KalshiMarketSnapshot(
                    ticker=ticker,
                    event_ticker=str(event.get("event_ticker", "")),
                    event_title=str(event.get("title", "")),
                    event_sub_title=str(event.get("sub_title", "")),
                    market_title=str(market.get("title", "")),
                    series_ticker=str(event.get("series_ticker", "")),
                    category=str(event.get("category", "")),
                    status=str(market.get("status", "")),
                    fetched_at=fetched_at,
                    occurrence_time=_parse_optional_datetime(
                        market.get("occurrence_datetime")
                    ),
                    market_type=str(market.get("market_type", "")),
                    yes_sub_title=str(market.get("yes_sub_title", "")),
                    no_sub_title=str(market.get("no_sub_title", "")),
                    rules_primary=str(market.get("rules_primary", "")),
                    yes_ask_levels=_merge_explicit_best_ask_level(
                        _parse_optional_decimal(market.get("yes_ask_dollars")),
                        _parse_optional_decimal(market.get("yes_ask_size_fp")),
                        orderbook["yes_ask_levels"],
                    ),
                    no_ask_levels=_merge_explicit_best_ask_level(
                        _parse_optional_decimal(market.get("no_ask_dollars")),
                        _parse_optional_decimal(market.get("no_ask_size_fp")),
                        orderbook["no_ask_levels"],
                    ),
                    yes_bid=orderbook["yes_bid"],
                    yes_bid_size=orderbook["yes_bid_size"],
                    yes_ask=_parse_optional_decimal(market.get("yes_ask_dollars"))
                    or orderbook["yes_ask"],
                    yes_ask_size=_parse_optional_decimal(market.get("yes_ask_size_fp"))
                    or orderbook["yes_ask_size"],
                    no_bid=orderbook["no_bid"],
                    no_bid_size=orderbook["no_bid_size"],
                    no_ask=_parse_optional_decimal(market.get("no_ask_dollars"))
                    or orderbook["no_ask"],
                    no_ask_size=_parse_optional_decimal(market.get("no_ask_size_fp"))
                    or orderbook["no_ask_size"],
                )
            )
            remaining -= 1

        return snapshots

    def _parse_levels(self, raw_levels: object) -> list[KalshiOrderLevel]:
        """Convert raw Kalshi price levels into normalized float levels."""

        if not isinstance(raw_levels, list):
            return []

        levels: list[KalshiOrderLevel] = []
        for raw_level in raw_levels:
            if not isinstance(raw_level, list) or len(raw_level) < 2:
                continue

            try:
                price = float(Decimal(str(raw_level[0])))
                size = float(Decimal(str(raw_level[1])))
            except (InvalidOperation, ValueError):
                continue

            levels.append(KalshiOrderLevel(price=price, size=size))

        levels.sort(key=lambda level: level.price, reverse=True)
        return levels

    def _derive_ask_levels(self, opposite_bid_levels: list[KalshiOrderLevel]) -> list[KalshiOrderLevel]:
        """Derive ask-side depth from the opposite-side bid ladder."""

        levels = [
            KalshiOrderLevel(price=1.0 - level.price, size=level.size)
            for level in opposite_bid_levels
        ]
        levels.sort(key=lambda level: level.price)
        return levels

    def _get_json(self, path: str, params: dict[str, str] | None = None) -> dict:
        """Issue a GET request and return JSON with consistent error handling."""

        url = f"{self._base_url}{path}"

        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, params=params)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise KalshiAPIError(f"Kalshi request failed for {path}: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise KalshiAPIError(f"Kalshi returned non-JSON data for {path}.") from exc

        if not isinstance(payload, dict):
            raise KalshiAPIError(f"Kalshi returned an unexpected payload for {path}.")

        return payload


def _parse_optional_datetime(value: object) -> datetime | None:
    """Parse an optional ISO 8601 datetime value."""

    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None

    return parsed.astimezone(UTC)


def _parse_optional_decimal(value: object) -> float | None:
    """Parse an optional decimal string or numeric value into a float."""

    if value is None:
        return None

    try:
        return float(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return None


def _merge_explicit_best_ask_level(
    explicit_price: float | None,
    explicit_size: float | None,
    derived_levels: tuple[KalshiOrderLevel, ...],
) -> tuple[KalshiOrderLevel, ...]:
    """Prefer explicit best-ask data while retaining depth from the derived ladder."""

    merged = list(derived_levels)
    if explicit_price is None or explicit_size is None:
        return tuple(merged)

    explicit_level = KalshiOrderLevel(price=explicit_price, size=explicit_size)
    if merged and abs(merged[0].price - explicit_price) < 1e-9:
        merged[0] = explicit_level
    else:
        merged.append(explicit_level)
        merged.sort(key=lambda level: level.price)
    return tuple(merged)


def _matchable_market_rejection_reason(event: dict, market: dict) -> str | None:
    """Return why a Kalshi market is rejected by the cheap prefilter, if at all."""

    if str(market.get("market_type", "")).lower() != "binary":
        return "non_binary_market"

    event_sub_title = str(event.get("sub_title", "")).strip()
    yes_sub_title = str(market.get("yes_sub_title", "")).strip()
    no_sub_title = str(market.get("no_sub_title", "")).strip()
    market_title = str(market.get("title", "")).strip()
    rules_primary = str(market.get("rules_primary", "")).strip()

    fields = (event_sub_title, yes_sub_title, market_title, rules_primary)
    if any(_contains_scope_text(field) for field in fields if field):
        return "scope_qualified_market"

    matchup = _parse_matchup_teams(event_sub_title)
    if matchup is None:
        return "event_subtitle_missing_matchup"

    event_team_a, event_team_b = matchup
    outcomes = _parse_outcome_teams(
        yes_sub_title,
        no_sub_title,
        event_team_a,
        event_team_b,
    )
    if outcomes is None:
        return "outcome_subtitles_unusable"

    return None


def _looks_like_matchable_team_market(event: dict, market: dict) -> bool:
    """Cheap prefilter to avoid fetching orderbooks for obviously irrelevant markets."""

    return _matchable_market_rejection_reason(event, market) is None


def _format_market_rejection_sample(event: dict, market: dict, reason: str) -> str:
    """Build a compact one-line sample for rejected Kalshi markets."""

    return (
        f"{reason}: ticker={market.get('ticker', '')} "
        f"event_sub_title={str(event.get('sub_title', '')).strip()!r} "
        f"market_title={str(market.get('title', '')).strip()!r} "
        f"yes_sub_title={str(market.get('yes_sub_title', '')).strip()!r} "
        f"no_sub_title={str(market.get('no_sub_title', '')).strip()!r}"
    )


def _parse_matchup_teams(event_sub_title: str) -> tuple[str, str] | None:
    """Extract team-vs-team pairings from a structured event subtitle."""

    cleaned = event_sub_title.strip()
    if not cleaned:
        return None

    patterns = (
        " vs ",
        " vs. ",
        " @ ",
        " at ",
    )
    lower = cleaned.lower()
    for pattern in patterns:
        if pattern in lower:
            left, right = re.split(pattern, cleaned, flags=re.IGNORECASE, maxsplit=1)
            left = left.strip()
            right = right.strip()
            if left and right:
                return left, right
    return None


def _parse_outcome_teams(
    yes_sub_title: str,
    no_sub_title: str,
    event_team_a: str,
    event_team_b: str,
) -> tuple[str, str] | None:
    """Extract YES/NO teams from outcome subtitles using safe winner-market shapes."""

    cleaned_yes = yes_sub_title.strip()
    cleaned_no = no_sub_title.strip()
    if not cleaned_yes:
        return None

    match = re.match(
        r"^(?P<yes_team>.+?)\s+beats\s+(?P<no_team>.+?)$",
        cleaned_yes,
        flags=re.IGNORECASE,
    )
    if match is not None:
        return match.group("yes_team").strip(), match.group("no_team").strip()

    normalized_event = {
        _normalize_comparison_text(event_team_a),
        _normalize_comparison_text(event_team_b),
    }
    normalized_yes = _normalize_comparison_text(cleaned_yes)
    normalized_no = _normalize_comparison_text(cleaned_no)
    if (
        cleaned_no
        and normalized_yes != normalized_no
        and {normalized_yes, normalized_no} == normalized_event
    ):
        return cleaned_yes, cleaned_no

    return None


def _contains_scope_text(value: str) -> bool:
    """Reject obvious non-full-game qualifiers before expensive orderbook fetches."""

    normalized = value.lower()
    disallowed_terms = (
        "round",
        "quarter",
        "period",
        "half",
        "inning",
        "map",
        "set",
        "season",
        "tournament",
        "matchup",
        "before ",
        "after ",
        "player",
        "draft",
        "rookie",
        "mvp",
        "top ",
        "pick ",
    )
    return any(term in normalized for term in disallowed_terms)


def _normalize_comparison_text(value: str) -> str:
    """Normalize team-like text for safe subtitle comparison."""

    normalized = value.lower().strip().replace("&", " and ")
    normalized = re.sub(r"[^\w\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized

    def fetch_active_sports_markets(self, limit: int = 25) -> list[KalshiMarketSnapshot]:
        """Fetch open sports markets and enrich them with best order-book prices.

        Args:
            limit: Maximum number of normalized market snapshots to return.

        Returns:
            A list of normalized market snapshots. Markets whose order books fail
            to load are skipped so a single bad market does not fail the batch.

        Raises:
            KalshiAPIError: If the initial market/event query fails.
        """

        query_limit = max(limit, 1)
        fetched_at = datetime.now(tz=UTC)
        params = {
            "status": "open",
            "with_nested_markets": "true",
            "limit": str(min(max(query_limit * 10, 100), MAX_KALSHI_EVENTS_PAGE_SIZE)),
        }

        snapshots: list[KalshiMarketSnapshot] = []
        cursor = ""

        while len(snapshots) < query_limit:
            if cursor:
                params["cursor"] = cursor
            elif "cursor" in params:
                del params["cursor"]

            payload = self._get_json("/events", params=params)
            events = payload.get("events")
            if not isinstance(events, list):
                raise KalshiAPIError("Kalshi response did not include a valid events list.")

            for event in events:
                if not isinstance(event, dict):
                    continue
                if str(event.get("category", "")).lower() != "sports":
                    continue

                markets = event.get("markets") or []
                for market in markets:
                    if not isinstance(market, dict):
                        continue
                    if str(market.get("status", "")).lower() not in {"open", "active"}:
                        continue

                    ticker = str(market.get("ticker", "")).strip()
                    if not ticker:
                        continue

                    try:
                        orderbook = self.fetch_orderbook(ticker)
                    except KalshiAPIError:
                        continue

                    snapshots.append(
                        KalshiMarketSnapshot(
                            ticker=ticker,
                            event_ticker=str(event.get("event_ticker", "")),
                            event_title=str(event.get("title", "")),
                            event_sub_title=str(event.get("sub_title", "")),
                            market_title=str(market.get("title", "")),
                            series_ticker=str(event.get("series_ticker", "")),
                            category=str(event.get("category", "")),
                            status=str(market.get("status", "")),
                            fetched_at=fetched_at,
                            occurrence_time=_parse_optional_datetime(
                                market.get("occurrence_datetime")
                            ),
                            market_type=str(market.get("market_type", "")),
                            yes_sub_title=str(market.get("yes_sub_title", "")),
                            no_sub_title=str(market.get("no_sub_title", "")),
                            rules_primary=str(market.get("rules_primary", "")),
                            yes_ask_levels=_merge_explicit_best_ask_level(
                                _parse_optional_decimal(market.get("yes_ask_dollars")),
                                _parse_optional_decimal(market.get("yes_ask_size_fp")),
                                orderbook["yes_ask_levels"],
                            ),
                            no_ask_levels=_merge_explicit_best_ask_level(
                                _parse_optional_decimal(market.get("no_ask_dollars")),
                                _parse_optional_decimal(market.get("no_ask_size_fp")),
                                orderbook["no_ask_levels"],
                            ),
                            yes_bid=orderbook["yes_bid"],
                            yes_bid_size=orderbook["yes_bid_size"],
                            yes_ask=_parse_optional_decimal(market.get("yes_ask_dollars"))
                            or orderbook["yes_ask"],
                            yes_ask_size=_parse_optional_decimal(market.get("yes_ask_size_fp"))
                            or orderbook["yes_ask_size"],
                            no_bid=orderbook["no_bid"],
                            no_bid_size=orderbook["no_bid_size"],
                            no_ask=_parse_optional_decimal(market.get("no_ask_dollars"))
                            or orderbook["no_ask"],
                            no_ask_size=_parse_optional_decimal(market.get("no_ask_size_fp"))
                            or orderbook["no_ask_size"],
                        )
                    )

                    if len(snapshots) >= query_limit:
                        return snapshots

            cursor = str(payload.get("cursor", "")).strip()
            if not cursor:
                break

        return snapshots

    def fetch_active_sports_markets_for_series(
        self,
        series_prefixes: tuple[str, ...],
        limit: int = 25,
    ) -> list[KalshiMarketSnapshot]:
        """Fetch open sports markets restricted to specific Kalshi series prefixes."""

        if not series_prefixes:
            return self.fetch_active_sports_markets(limit=limit)

        query_limit = max(limit, 1)
        fetched_at = datetime.now(tz=UTC)
        params = {
            "status": "open",
            "with_nested_markets": "true",
            "limit": str(min(max(query_limit * 10, 100), MAX_KALSHI_EVENTS_PAGE_SIZE)),
        }

        snapshots: list[KalshiMarketSnapshot] = []
        rejection_counts: Counter[str] = Counter()
        rejection_samples: list[str] = []
        cursor = ""

        while len(snapshots) < query_limit:
            if cursor:
                params["cursor"] = cursor
            elif "cursor" in params:
                del params["cursor"]

            payload = self._get_json("/events", params=params)
            events = payload.get("events")
            if not isinstance(events, list):
                raise KalshiAPIError("Kalshi response did not include a valid events list.")

            for event in events:
                if not isinstance(event, dict):
                    continue
                if str(event.get("category", "")).lower() != "sports":
                    continue
                series_ticker = str(event.get("series_ticker", ""))
                if not any(series_ticker.startswith(prefix) for prefix in series_prefixes):
                    continue

                snapshots.extend(
                    self._normalize_event_markets(
                        event=event,
                        fetched_at=fetched_at,
                        remaining=query_limit - len(snapshots),
                        rejection_counts=rejection_counts,
                        rejection_samples=rejection_samples,
                    )
                )
                if len(snapshots) >= query_limit:
                    return snapshots

            cursor = str(payload.get("cursor", "")).strip()
            if not cursor:
                break

        if not snapshots and rejection_counts:
            logger.info(
                "Kalshi prefilter rejected all candidate markets for series=%s. Reasons=%s. Samples=%s",
                ",".join(series_prefixes),
                dict(rejection_counts.most_common()),
                rejection_samples,
            )

        return snapshots

    def fetch_orderbook(self, ticker: str) -> dict[str, float | None]:
        """Fetch and normalize a single market order book.

        Kalshi returns YES bids and NO bids only. In a binary market, the best
        ask on one side is implied by the best bid on the opposite side.

        Args:
            ticker: Market ticker.

        Returns:
            A normalized dictionary containing best bid, best ask, and available
            size for both YES and NO sides.

        Raises:
            KalshiAPIError: If the orderbook request fails or the payload is malformed.
        """

        payload = self._get_json(f"/markets/{ticker}/orderbook")
        orderbook = payload.get("orderbook_fp")
        if not isinstance(orderbook, dict):
            raise KalshiAPIError(f"Kalshi orderbook for {ticker} was missing orderbook_fp.")

        yes_levels = self._parse_levels(orderbook.get("yes_dollars"))
        no_levels = self._parse_levels(orderbook.get("no_dollars"))

        best_yes_bid = yes_levels[0] if yes_levels else None
        best_no_bid = no_levels[0] if no_levels else None

        yes_ask_levels = self._derive_ask_levels(no_levels)
        no_ask_levels = self._derive_ask_levels(yes_levels)
        best_yes_ask = yes_ask_levels[0] if yes_ask_levels else None
        best_no_ask = no_ask_levels[0] if no_ask_levels else None

        return {
            "yes_bid": best_yes_bid.price if best_yes_bid else None,
            "yes_bid_size": best_yes_bid.size if best_yes_bid else None,
            "yes_ask": best_yes_ask.price if best_yes_ask else None,
            "yes_ask_size": best_yes_ask.size if best_yes_ask else None,
            "yes_ask_levels": tuple(yes_ask_levels),
            "no_bid": best_no_bid.price if best_no_bid else None,
            "no_bid_size": best_no_bid.size if best_no_bid else None,
            "no_ask": best_no_ask.price if best_no_ask else None,
            "no_ask_size": best_no_ask.size if best_no_ask else None,
            "no_ask_levels": tuple(no_ask_levels),
        }

    def _normalize_event_markets(
        self,
        event: dict,
        fetched_at: datetime,
        remaining: int,
        rejection_counts: Counter[str] | None = None,
        rejection_samples: list[str] | None = None,
    ) -> list[KalshiMarketSnapshot]:
        """Normalize nested market payloads for one Kalshi event."""

        snapshots: list[KalshiMarketSnapshot] = []
        markets = event.get("markets") or []
        for market in markets:
            if remaining <= 0:
                break
            if not isinstance(market, dict):
                continue
            if str(market.get("status", "")).lower() not in {"open", "active"}:
                continue
            rejection_reason = _matchable_market_rejection_reason(event, market)
            if rejection_reason is not None:
                if rejection_counts is not None:
                    rejection_counts[rejection_reason] += 1
                if rejection_samples is not None and len(rejection_samples) < MAX_REJECTION_SAMPLES:
                    rejection_samples.append(
                        _format_market_rejection_sample(event, market, rejection_reason)
                    )
                continue

            ticker = str(market.get("ticker", "")).strip()
            if not ticker:
                continue

            try:
                orderbook = self.fetch_orderbook(ticker)
            except KalshiAPIError:
                continue

            snapshots.append(
                KalshiMarketSnapshot(
                    ticker=ticker,
                    event_ticker=str(event.get("event_ticker", "")),
                    event_title=str(event.get("title", "")),
                    event_sub_title=str(event.get("sub_title", "")),
                    market_title=str(market.get("title", "")),
                    series_ticker=str(event.get("series_ticker", "")),
                    category=str(event.get("category", "")),
                    status=str(market.get("status", "")),
                    fetched_at=fetched_at,
                    occurrence_time=_parse_optional_datetime(
                        market.get("occurrence_datetime")
                    ),
                    market_type=str(market.get("market_type", "")),
                    yes_sub_title=str(market.get("yes_sub_title", "")),
                    no_sub_title=str(market.get("no_sub_title", "")),
                    rules_primary=str(market.get("rules_primary", "")),
                    yes_ask_levels=_merge_explicit_best_ask_level(
                        _parse_optional_decimal(market.get("yes_ask_dollars")),
                        _parse_optional_decimal(market.get("yes_ask_size_fp")),
                        orderbook["yes_ask_levels"],
                    ),
                    no_ask_levels=_merge_explicit_best_ask_level(
                        _parse_optional_decimal(market.get("no_ask_dollars")),
                        _parse_optional_decimal(market.get("no_ask_size_fp")),
                        orderbook["no_ask_levels"],
                    ),
                    yes_bid=orderbook["yes_bid"],
                    yes_bid_size=orderbook["yes_bid_size"],
                    yes_ask=_parse_optional_decimal(market.get("yes_ask_dollars"))
                    or orderbook["yes_ask"],
                    yes_ask_size=_parse_optional_decimal(market.get("yes_ask_size_fp"))
                    or orderbook["yes_ask_size"],
                    no_bid=orderbook["no_bid"],
                    no_bid_size=orderbook["no_bid_size"],
                    no_ask=_parse_optional_decimal(market.get("no_ask_dollars"))
                    or orderbook["no_ask"],
                    no_ask_size=_parse_optional_decimal(market.get("no_ask_size_fp"))
                    or orderbook["no_ask_size"],
                )
            )
            remaining -= 1

        return snapshots

    def _parse_levels(self, raw_levels: object) -> list[KalshiOrderLevel]:
        """Convert raw Kalshi price levels into normalized float levels."""

        if not isinstance(raw_levels, list):
            return []

        levels: list[KalshiOrderLevel] = []
        for raw_level in raw_levels:
            if not isinstance(raw_level, list) or len(raw_level) < 2:
                continue

            try:
                price = float(Decimal(str(raw_level[0])))
                size = float(Decimal(str(raw_level[1])))
            except (InvalidOperation, ValueError):
                continue

            levels.append(KalshiOrderLevel(price=price, size=size))

        levels.sort(key=lambda level: level.price, reverse=True)
        return levels

    def _derive_ask_levels(self, opposite_bid_levels: list[KalshiOrderLevel]) -> list[KalshiOrderLevel]:
        """Derive ask-side depth from the opposite-side bid ladder."""

        levels = [
            KalshiOrderLevel(price=1.0 - level.price, size=level.size)
            for level in opposite_bid_levels
        ]
        levels.sort(key=lambda level: level.price)
        return levels

    def _get_json(self, path: str, params: dict[str, str] | None = None) -> dict:
        """Issue a GET request and return JSON with consistent error handling."""

        url = f"{self._base_url}{path}"

        try:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.get(url, params=params)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise KalshiAPIError(f"Kalshi request failed for {path}: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise KalshiAPIError(f"Kalshi returned non-JSON data for {path}.") from exc

        if not isinstance(payload, dict):
            raise KalshiAPIError(f"Kalshi returned an unexpected payload for {path}.")

        return payload


def _parse_optional_datetime(value: object) -> datetime | None:
    """Parse an optional ISO 8601 datetime value."""

    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None

    return parsed.astimezone(UTC)


def _parse_optional_decimal(value: object) -> float | None:
    """Parse an optional decimal string or numeric value into a float."""

    if value is None:
        return None

    try:
        return float(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return None


def _merge_explicit_best_ask_level(
    explicit_price: float | None,
    explicit_size: float | None,
    derived_levels: tuple[KalshiOrderLevel, ...],
) -> tuple[KalshiOrderLevel, ...]:
    """Prefer explicit best-ask data while retaining depth from the derived ladder."""

    merged = list(derived_levels)
    if explicit_price is None or explicit_size is None:
        return tuple(merged)

    explicit_level = KalshiOrderLevel(price=explicit_price, size=explicit_size)
    if merged and abs(merged[0].price - explicit_price) < 1e-9:
        merged[0] = explicit_level
    else:
        merged.append(explicit_level)
        merged.sort(key=lambda level: level.price)
    return tuple(merged)


def _matchable_market_rejection_reason(event: dict, market: dict) -> str | None:
    """Return why a Kalshi market is rejected by the cheap prefilter, if at all."""

    if str(market.get("market_type", "")).lower() != "binary":
        return "non_binary_market"

    event_sub_title = str(event.get("sub_title", "")).strip()
    yes_sub_title = str(market.get("yes_sub_title", "")).strip()
    market_title = str(market.get("title", "")).strip()
    rules_primary = str(market.get("rules_primary", "")).strip()

    if " vs " not in event_sub_title.lower():
        return "event_subtitle_missing_vs"
    if " beats " not in yes_sub_title.lower():
        return "yes_subtitle_missing_beats"
    if " beat " not in market_title.lower():
        return "market_title_missing_beat"

    fields = (event_sub_title, yes_sub_title, market_title, rules_primary)
    if any(_contains_scope_text(field) for field in fields if field):
        return "scope_qualified_market"

    return None


def _looks_like_matchable_team_market(event: dict, market: dict) -> bool:
    """Cheap prefilter to avoid fetching orderbooks for obviously irrelevant markets."""

    return _matchable_market_rejection_reason(event, market) is None


def _format_market_rejection_sample(event: dict, market: dict, reason: str) -> str:
    """Build a compact one-line sample for rejected Kalshi markets."""

    return (
        f"{reason}: ticker={market.get('ticker', '')} "
        f"event_sub_title={str(event.get('sub_title', '')).strip()!r} "
        f"market_title={str(market.get('title', '')).strip()!r} "
        f"yes_sub_title={str(market.get('yes_sub_title', '')).strip()!r}"
    )


def _contains_scope_text(value: str) -> bool:
    """Reject obvious non-full-game qualifiers before expensive orderbook fetches."""

    normalized = value.lower()
    disallowed_terms = (
        "round",
        "quarter",
        "period",
        "half",
        "inning",
        "map",
        "set",
        "season",
        "tournament",
        "matchup",
        "before ",
        "after ",
        "player",
        "draft",
        "rookie",
        "mvp",
        "top ",
        "pick ",
    )
    return any(term in normalized for term in disallowed_terms)
