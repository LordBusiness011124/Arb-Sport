"""Runnable polling service for sportsbook vs Kalshi edge detection."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from arb.alerts.console import format_opportunity
from arb.alerts.discord import DiscordAlertError, send_discord_alerts
from arb.clients.kalshi import KalshiAPIError, KalshiClient
from arb.clients.sportsbook import SportsbookAPIError, SportsbookClient
from arb.config import Settings, load_settings
from arb.core.matching import (
    MatchResult,
    match_sportsbook_game_to_kalshi_market,
    normalize_team_name,
)
from arb.core.pricing import (
    BookValidationError,
    LiquidityError,
    calculate_fill_price,
    calculate_raw_edge,
    estimate_net_edge,
    select_executable_price,
    validate_market_book,
)
from arb.models.market import KalshiEventMarket, KalshiMarketSnapshot
from arb.models.odds import SportsbookMoneylineMarket
from arb.models.opportunity import Opportunity
from arb.services.storage import (
    connect_sqlite,
    create_scan_run,
    initialize_schema,
    prune_old_scan_data,
    store_kalshi_markets,
    store_opportunities,
    store_sportsbook_markets,
)


logger = logging.getLogger(__name__)

LEAGUE_NAME_MAP = {
    "basketball_ncaab": "college basketball",
    "americanfootball_ncaaf": "college football",
    "basketball_nba": "nba",
    "americanfootball_nfl": "nfl",
    "icehockey_nhl": "nhl",
    "baseball_mlb": "mlb",
}

KALSHI_SERIES_LEAGUE_MAP = {
    "KXNBA": "nba",
    "KXBBL": "nba",
    "KXNHL": "nhl",
    "KXMLB": "mlb",
    "KXNFL": "nfl",
    "KXNCAAB": "college basketball",
    "KXNCAAF": "college football",
    "KXPGA": "golf",
}

SPORT_TO_KALSHI_SERIES_PREFIXES = {
    "basketball_ncaab": ("KXNCAAB",),
    "basketball_nba": ("KXNBA", "KXBBL"),
    "americanfootball_ncaaf": ("KXNCAAF",),
    "americanfootball_nfl": ("KXNFL",),
    "icehockey_nhl": ("KXNHL",),
    "baseball_mlb": ("KXMLB",),
}


@dataclass(slots=True)
class MatchableKalshiMarket:
    """Kalshi market with enough structure to evaluate an edge."""

    event_market: KalshiEventMarket
    snapshot: KalshiMarketSnapshot
    yes_team: str
    no_team: str


@dataclass(slots=True)
class RejectedMatch:
    """Diagnostic for why a sportsbook market was not matched safely."""

    reason: str
    candidates: int = 0


def main() -> None:
    """Run the long-lived polling loop."""

    settings = load_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    sportsbook_client = SportsbookClient(
        base_url=settings.sportsbook_base_url,
        api_key=settings.sportsbook_api_key,
        bookmaker=settings.sportsbook_name,
        sport=settings.sportsbook_sport,
        regions=settings.sportsbook_regions,
    )
    kalshi_client = KalshiClient(base_url=settings.kalshi_base_url)
    connection = connect_sqlite(settings.sqlite_path)
    initialize_schema(connection)

    sportsbook_cache: list[SportsbookMoneylineMarket] = []
    kalshi_cache: list[KalshiMarketSnapshot] = []
    last_sportsbook_poll: float = 0.0
    last_kalshi_poll: float = 0.0
    last_scan_inputs: tuple[int, int] | None = None

    logger.info(
        "Starting service loop with sportsbook_poll=%ss kalshi_poll=%ss",
        settings.sportsbook_poll_seconds,
        settings.kalshi_poll_seconds,
    )

    while True:
        now = time.monotonic()
        sportsbook_updated = False
        kalshi_updated = False

        if now - last_sportsbook_poll >= settings.sportsbook_poll_seconds:
            sportsbook_cache = _poll_sportsbook(sportsbook_client)
            last_sportsbook_poll = now
            sportsbook_updated = True

        if now - last_kalshi_poll >= settings.kalshi_poll_seconds:
            kalshi_cache = _poll_kalshi(kalshi_client, settings)
            last_kalshi_poll = now
            kalshi_updated = True

        if sportsbook_cache and kalshi_cache and (sportsbook_updated or kalshi_updated):
            current_inputs = (len(sportsbook_cache), len(kalshi_cache))
            if current_inputs != last_scan_inputs or sportsbook_updated or kalshi_updated:
                _run_scan(connection, settings, sportsbook_cache, kalshi_cache)
                last_scan_inputs = current_inputs

        time.sleep(settings.loop_sleep_seconds)


def _poll_sportsbook(client: SportsbookClient) -> list[SportsbookMoneylineMarket]:
    """Fetch sportsbook odds with graceful failure handling."""

    try:
        markets = client.fetch_moneyline_markets()
        logger.info("Fetched %s sportsbook markets", len(markets))
        return markets
    except SportsbookAPIError as exc:
        logger.warning("Sportsbook poll failed: %s", exc)
        return []


def _poll_kalshi(client: KalshiClient, settings: Settings) -> list[KalshiMarketSnapshot]:
    """Fetch Kalshi market data with graceful failure handling."""

    try:
        series_prefixes = SPORT_TO_KALSHI_SERIES_PREFIXES.get(settings.sportsbook_sport, ())
        markets = client.fetch_active_sports_markets_for_series(
            series_prefixes=series_prefixes,
            limit=settings.kalshi_market_limit,
        )
        logger.info("Fetched %s Kalshi markets", len(markets))
        return markets
    except KalshiAPIError as exc:
        logger.warning("Kalshi poll failed: %s", exc)
        return []


def _run_scan(
    connection,
    settings: Settings,
    sportsbook_markets: list[SportsbookMoneylineMarket],
    kalshi_snapshots: list[KalshiMarketSnapshot],
) -> None:
    """Match events, compute edges, print alerts, and store a scan."""

    started_at = datetime.now(tz=UTC)
    opportunities: list[Opportunity] = []
    matched_event_count = 0
    candidates = [
        candidate
        for snapshot in kalshi_snapshots
        if (candidate := _build_matchable_kalshi_market(snapshot, settings.sportsbook_sport))
    ]

    for sportsbook_market in sportsbook_markets:
        best_match = _select_unique_match(
            sportsbook_market=sportsbook_market,
            candidates=candidates,
            settings=settings,
        )
        if best_match is None:
            continue

        matched_event_count += 1
        candidate, match_result = best_match

        opportunities.extend(
            _build_opportunities_for_match(
                sportsbook_market=sportsbook_market,
                candidate=candidate,
                match_result=match_result,
                settings=settings,
                detected_at=started_at,
            )
        )

    scan_run_id = create_scan_run(
        connection,
        started_at=started_at,
        sportsbook_market_count=len(sportsbook_markets),
        kalshi_market_count=len(kalshi_snapshots),
        matched_event_count=matched_event_count,
        opportunity_count=len(opportunities),
    )
    store_sportsbook_markets(connection, scan_run_id, sportsbook_markets)
    store_kalshi_markets(connection, scan_run_id, kalshi_snapshots)
    if opportunities:
        store_opportunities(connection, scan_run_id, opportunities)
        for opportunity in opportunities:
            print(format_opportunity(opportunity))
        if settings.discord_webhook_url:
            try:
                send_discord_alerts(
                    webhook_url=settings.discord_webhook_url,
                    opportunities=opportunities,
                    username=settings.discord_username,
                )
            except DiscordAlertError as exc:
                logger.warning("Discord alert delivery failed: %s", exc)

    removed_scan_runs = prune_old_scan_data(
        connection,
        retention_days=settings.sqlite_retention_days,
        now=started_at,
    )
    if removed_scan_runs:
        logger.info(
            "Pruned %s old scan run(s) older than %s day(s)",
            removed_scan_runs,
            settings.sqlite_retention_days,
        )

    logger.info(
        "Completed scan: sportsbook=%s kalshi=%s matched=%s opportunities=%s",
        len(sportsbook_markets),
        len(kalshi_snapshots),
        matched_event_count,
        len(opportunities),
    )


def _build_opportunities_for_match(
    sportsbook_market: SportsbookMoneylineMarket,
    candidate: MatchableKalshiMarket,
    match_result: MatchResult,
    settings: Settings,
    detected_at: datetime,
) -> list[Opportunity]:
    """Build opportunities for both teams in one matched game."""

    team_probabilities = {
        normalize_team_name(sportsbook_market.game.home_team): sportsbook_market.home_fair_probability,
        normalize_team_name(sportsbook_market.game.away_team): sportsbook_market.away_fair_probability,
    }

    opportunities: list[Opportunity] = []
    side_map = {
        "yes": normalize_team_name(candidate.yes_team),
        "no": normalize_team_name(candidate.no_team),
    }

    for side, normalized_team in side_map.items():
        if settings.alert_only_yes_signals and side != "yes":
            continue

        fair_probability = team_probabilities.get(normalized_team)
        if fair_probability is None:
            continue

        try:
            validate_market_book(
                candidate.snapshot,
                max_dislocation=settings.max_book_dislocation,
            )
            top_price, top_size = select_executable_price(
                candidate.snapshot,
                side,
                minimum_size=settings.minimum_liquidity,
            )
            executable_price, total_available = calculate_fill_price(
                candidate.snapshot,
                side,
                target_size=settings.target_order_size,
            )
        except (ValueError, LiquidityError, BookValidationError):
            continue

        raw_edge = calculate_raw_edge(fair_probability, executable_price)
        net_edge = estimate_net_edge(
            fair_probability,
            executable_price,
            fee_rate=settings.fee_rate,
            slippage=settings.slippage,
        )
        if net_edge <= settings.edge_threshold:
            continue

        opportunities.append(
            Opportunity(
                sportsbook_game_id=sportsbook_market.game.game_id,
                event_label=(
                    f"{sportsbook_market.game.home_team} vs "
                    f"{sportsbook_market.game.away_team}"
                ),
                kalshi_ticker=candidate.snapshot.ticker,
                side=side,
                team=candidate.yes_team if side == "yes" else candidate.no_team,
                fair_probability=fair_probability,
                executable_price=executable_price,
                available_size=total_available,
                raw_edge=raw_edge,
                net_edge=net_edge,
                match_confidence=match_result.confidence_score,
                explanation=match_result.explanation,
                detected_at=detected_at,
            )
        )

    return opportunities


def _select_unique_match(
    sportsbook_market: SportsbookMoneylineMarket,
    candidates: list[MatchableKalshiMarket],
    settings: Settings,
) -> tuple[MatchableKalshiMarket, MatchResult] | None:
    """Return one safe candidate or reject the game as stale/ambiguous."""

    valid_matches: list[tuple[MatchableKalshiMarket, MatchResult]] = []
    for candidate in candidates:
        freshness_rejection = _reject_stale_or_skewed_match(
            sportsbook_market=sportsbook_market,
            kalshi_snapshot=candidate.snapshot,
            settings=settings,
        )
        if freshness_rejection is not None:
            continue

        result = match_sportsbook_game_to_kalshi_market(
            sportsbook_market.game,
            candidate.event_market,
            max_start_time_diff_minutes=settings.max_match_time_diff_minutes,
            min_confidence_score=settings.match_confidence_threshold,
        )
        if result.match:
            valid_matches.append((candidate, result))

    if not valid_matches:
        return None

    valid_matches.sort(key=lambda item: item[1].confidence_score, reverse=True)
    best_candidate, best_result = valid_matches[0]
    if len(valid_matches) == 1:
        return best_candidate, best_result

    second_best = valid_matches[1][1].confidence_score
    if best_result.confidence_score - second_best < settings.ambiguous_match_confidence_delta:
        logger.info(
            "Rejected ambiguous match for sportsbook game %s: top two confidences %.4f and %.4f",
            sportsbook_market.game.game_id,
            best_result.confidence_score,
            second_best,
        )
        return None

    return best_candidate, best_result


def _reject_stale_or_skewed_match(
    sportsbook_market: SportsbookMoneylineMarket,
    kalshi_snapshot: KalshiMarketSnapshot,
    settings: Settings,
    now: datetime | None = None,
) -> RejectedMatch | None:
    """Reject a potential comparison if either side is stale or too far apart in time."""

    current_time = now or datetime.now(tz=UTC)
    sportsbook_age_seconds = (current_time - sportsbook_market.fetched_at).total_seconds()
    if sportsbook_age_seconds > settings.max_sportsbook_snapshot_age_seconds:
        return RejectedMatch("sportsbook_snapshot_stale")

    kalshi_age_seconds = (current_time - kalshi_snapshot.fetched_at).total_seconds()
    if kalshi_age_seconds > settings.max_kalshi_snapshot_age_seconds:
        return RejectedMatch("kalshi_snapshot_stale")

    cross_feed_skew_seconds = abs(
        (sportsbook_market.fetched_at - kalshi_snapshot.fetched_at).total_seconds()
    )
    if cross_feed_skew_seconds > settings.max_cross_feed_skew_seconds:
        return RejectedMatch("cross_feed_skew_too_large")

    return None


def _build_matchable_kalshi_market(
    snapshot: KalshiMarketSnapshot,
    sportsbook_sport: str,
) -> MatchableKalshiMarket | None:
    """Parse a Kalshi market into a matchable team-vs-team candidate.

    This parser is intentionally strict. It only accepts markets whose
    structured fields describe a clean full-game team-vs-team binary contract.
    Anything with round/period/season/tournament qualifiers is rejected.
    """

    if snapshot.market_type.lower() != "binary":
        return None

    inferred_league = _infer_kalshi_league(snapshot)
    target_league = LEAGUE_NAME_MAP.get(sportsbook_sport, sportsbook_sport)
    if inferred_league != target_league:
        return None

    if snapshot.occurrence_time is None:
        return None

    if _contains_scope_qualifier(snapshot):
        return None

    teams = (
        _parse_matchup_from_event_text(snapshot.event_sub_title)
        or _parse_matchup_from_event_text(snapshot.event_title)
    )
    if teams is None:
        return None
    event_team_a, event_team_b = teams

    resolution_teams = _parse_resolution_matchup(
        snapshot.yes_sub_title,
        snapshot.no_sub_title,
        snapshot.market_title,
        event_team_a,
        event_team_b,
    )
    if resolution_teams is None:
        return None
    yes_team, no_team = resolution_teams

    normalized_event_teams = {normalize_team_name(event_team_a), normalize_team_name(event_team_b)}
    normalized_resolution_teams = {normalize_team_name(yes_team), normalize_team_name(no_team)}
    if normalized_event_teams != normalized_resolution_teams:
        return None

    if normalize_team_name(yes_team) == normalize_team_name(no_team):
        return None

    event_market = KalshiEventMarket(
        ticker=snapshot.ticker,
        league=inferred_league,
        team_a=yes_team,
        team_b=no_team,
        start_time=snapshot.occurrence_time,
        title=snapshot.market_title,
    )
    return MatchableKalshiMarket(
        event_market=event_market,
        snapshot=snapshot,
        yes_team=yes_team,
        no_team=no_team,
    )


def _parse_matchup_from_event_text(event_text: str) -> tuple[str, str] | None:
    """Extract a clean team-vs-team pairing from event title/subtitle text."""

    cleaned = event_text.strip()
    if not cleaned or _contains_scope_text(cleaned):
        return None

    patterns = (
        r"^(?P<team_a>.+?)\s+vs\.?\s+(?P<team_b>.+?)$",
        r"^(?P<team_a>.+?)\s+@\s+(?P<team_b>.+?)$",
        r"^(?P<team_a>.+?)\s+at\s+(?P<team_b>.+?)$",
    )
    for pattern in patterns:
        match = re.match(pattern, cleaned, flags=re.IGNORECASE)
        if match is not None:
            return match.group("team_a").strip(), match.group("team_b").strip()
    return None


def _parse_resolution_matchup(
    yes_sub_title: str,
    no_sub_title: str,
    market_title: str,
    event_team_a: str,
    event_team_b: str,
) -> tuple[str, str] | None:
    """Extract YES/NO teams from structured outcome subtitles.

    Supported safe forms:
    - ``Team A beats Team B``
    - ``yes_sub_title`` and ``no_sub_title`` are the two team names directly
    """

    cleaned_yes = yes_sub_title.strip()
    cleaned_no = no_sub_title.strip()
    if not cleaned_yes or _contains_scope_text(cleaned_yes):
        return None
    if cleaned_no and _contains_scope_text(cleaned_no):
        return None

    match = re.match(
        r"^(?P<yes_team>.+?)\s+beats\s+(?P<no_team>.+?)$",
        cleaned_yes,
        flags=re.IGNORECASE,
    )
    if match is not None:
        return match.group("yes_team").strip(), match.group("no_team").strip()

    title_match = re.match(
        r"^will\s+(?P<yes_team>.+?)\s+beat\s+(?P<no_team>.+?)\??$",
        market_title.strip(),
        flags=re.IGNORECASE,
    )
    if title_match is not None:
        return title_match.group("yes_team").strip(), title_match.group("no_team").strip()

    normalized_event = {normalize_team_name(event_team_a), normalize_team_name(event_team_b)}
    normalized_yes = normalize_team_name(cleaned_yes)
    normalized_no = normalize_team_name(cleaned_no)
    if (
        cleaned_no
        and normalized_yes != normalized_no
        and {normalized_yes, normalized_no} == normalized_event
    ):
        return cleaned_yes, cleaned_no

    return None


def _contains_scope_qualifier(snapshot: KalshiMarketSnapshot) -> bool:
    """Return whether structured market fields imply non-full-game scope."""

    fields = [
        snapshot.event_title,
        snapshot.event_sub_title,
        snapshot.market_title,
        snapshot.yes_sub_title,
        snapshot.no_sub_title,
        snapshot.rules_primary,
    ]
    return any(_contains_scope_text(field) for field in fields if field)


def _contains_scope_text(value: str) -> bool:
    """Reject structured text that narrows the contract below full-game scope."""

    normalized = value.lower()
    disallowed_terms = (
        "round",
        "quarter",
        "period",
        "half",
        "inning",
        "map",
        "set",
        "game ",
        "season",
        "tournament",
        "matchup",
        "before ",
        "after ",
        "player",
    )
    return any(term in normalized for term in disallowed_terms)

def _infer_kalshi_league(snapshot: KalshiMarketSnapshot) -> str | None:
    """Infer a league name conservatively from Kalshi series/event tickers."""

    for prefix, league in KALSHI_SERIES_LEAGUE_MAP.items():
        if snapshot.series_ticker.startswith(prefix) or snapshot.event_ticker.startswith(prefix):
            return league
    return None


if __name__ == "__main__":
    main()
