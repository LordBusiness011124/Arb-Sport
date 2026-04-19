"""Tests for executable-price selection and edge calculations."""

from datetime import UTC, datetime

import pytest

from arb.core.pricing import (
    BookValidationError,
    LiquidityError,
    calculate_fill_price,
    calculate_raw_edge,
    estimate_net_edge,
    select_executable_price,
    validate_market_book,
)
from arb.models.market import KalshiMarketSnapshot, KalshiOrderLevel


def build_market_snapshot() -> KalshiMarketSnapshot:
    """Create a reusable normalized market snapshot for pricing tests."""

    return KalshiMarketSnapshot(
        ticker="TEST-MARKET",
        event_ticker="TEST-EVENT",
        event_title="Test event",
        event_sub_title="North Carolina vs Duke",
        market_title="Test market",
        series_ticker="TEST",
        category="Sports",
        status="active",
        fetched_at=datetime(2026, 4, 20, 0, 0, tzinfo=UTC),
        occurrence_time=None,
        market_type="binary",
        yes_sub_title="North Carolina beats Duke",
        no_sub_title="North Carolina beats Duke",
        rules_primary="If North Carolina wins, then the market resolves to Yes.",
        yes_ask_levels=(
            KalshiOrderLevel(price=0.60, size=40.0),
            KalshiOrderLevel(price=0.62, size=20.0),
        ),
        no_ask_levels=(
            KalshiOrderLevel(price=0.40, size=30.0),
            KalshiOrderLevel(price=0.42, size=15.0),
        ),
        yes_bid=0.58,
        yes_bid_size=25.0,
        yes_ask=0.60,
        yes_ask_size=40.0,
        no_bid=0.38,
        no_bid_size=20.0,
        no_ask=0.40,
        no_ask_size=30.0,
    )


def test_select_executable_price_for_yes_returns_best_ask_and_size() -> None:
    market = build_market_snapshot()

    price, size = select_executable_price(market, "yes")

    assert price == pytest.approx(0.60)
    assert size == pytest.approx(40.0)


def test_select_executable_price_for_no_returns_best_ask_and_size() -> None:
    market = build_market_snapshot()

    price, size = select_executable_price(market, "no")

    assert price == pytest.approx(0.40)
    assert size == pytest.approx(30.0)


def test_select_executable_price_rejects_invalid_side() -> None:
    market = build_market_snapshot()

    with pytest.raises(ValueError, match="either 'yes' or 'no'"):
        select_executable_price(market, "maybe")


def test_select_executable_price_rejects_missing_ask() -> None:
    market = build_market_snapshot()
    market.yes_ask = None

    with pytest.raises(ValueError, match="No executable YES ask"):
        select_executable_price(market, "yes")


def test_select_executable_price_rejects_missing_size() -> None:
    market = build_market_snapshot()
    market.no_ask_size = None

    with pytest.raises(ValueError, match="No executable NO ask"):
        select_executable_price(market, "no")


def test_select_executable_price_rejects_low_liquidity() -> None:
    market = build_market_snapshot()

    with pytest.raises(LiquidityError, match="below minimum"):
        select_executable_price(market, "yes", minimum_size=50.0)


def test_select_executable_price_rejects_negative_minimum_size() -> None:
    market = build_market_snapshot()

    with pytest.raises(ValueError, match="cannot be negative"):
        select_executable_price(market, "yes", minimum_size=-1)


def test_calculate_raw_edge_returns_probability_difference() -> None:
    assert calculate_raw_edge(0.64, 0.60) == pytest.approx(0.04)


def test_calculate_fill_price_uses_vwap_for_target_size() -> None:
    market = build_market_snapshot()

    fill_price, total_available = calculate_fill_price(market, "yes", target_size=50.0)

    assert fill_price == pytest.approx((40 * 0.60 + 10 * 0.62) / 50)
    assert total_available == pytest.approx(60.0)


def test_calculate_fill_price_rejects_insufficient_depth() -> None:
    market = build_market_snapshot()

    with pytest.raises(LiquidityError, match="below target"):
        calculate_fill_price(market, "yes", target_size=100.0)


def test_calculate_fill_price_rejects_invalid_target_size() -> None:
    market = build_market_snapshot()

    with pytest.raises(ValueError, match="must be positive"):
        calculate_fill_price(market, "yes", target_size=0)


def test_validate_market_book_rejects_crossed_yes_book() -> None:
    market = build_market_snapshot()
    market.yes_ask = 0.57

    with pytest.raises(BookValidationError, match="YES ask is below YES bid"):
        validate_market_book(market)


def test_validate_market_book_rejects_large_explicit_ladder_dislocation() -> None:
    market = build_market_snapshot()
    market.yes_ask = 0.66

    with pytest.raises(BookValidationError, match="explicit best ask differs"):
        validate_market_book(market, max_dislocation=0.02)


def test_validate_market_book_accepts_consistent_snapshot() -> None:
    market = build_market_snapshot()

    validate_market_book(market, max_dislocation=0.02)


@pytest.mark.parametrize(
    ("fair_probability", "executable_price"),
    [(-0.1, 0.5), (1.1, 0.5), (0.5, -0.1), (0.5, 1.1)],
)
def test_calculate_raw_edge_rejects_invalid_probabilities(
    fair_probability: float,
    executable_price: float,
) -> None:
    with pytest.raises(ValueError, match="must be between 0 and 1"):
        calculate_raw_edge(fair_probability, executable_price)


def test_estimate_net_edge_subtracts_fee_and_slippage() -> None:
    net_edge = estimate_net_edge(
        fair_probability=0.64,
        executable_price=0.60,
        fee_rate=0.02,
        slippage=0.01,
    )

    assert net_edge == pytest.approx(0.018)


def test_estimate_net_edge_defaults_to_raw_edge_when_no_costs() -> None:
    assert estimate_net_edge(0.64, 0.60) == pytest.approx(0.04)


@pytest.mark.parametrize(("fee_rate", "slippage"), [(-0.01, 0.0), (0.01, -0.01)])
def test_estimate_net_edge_rejects_negative_cost_inputs(
    fee_rate: float,
    slippage: float,
) -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        estimate_net_edge(0.64, 0.60, fee_rate=fee_rate, slippage=slippage)
