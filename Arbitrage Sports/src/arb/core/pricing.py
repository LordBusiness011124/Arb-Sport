"""Core edge-calculation helpers for Kalshi binary markets.

Assumptions:
1. The trade is evaluated from the perspective of buying a contract.
2. Buying ``YES`` executes at ``yes_ask``.
3. Buying ``NO`` executes at ``no_ask``.
4. ``fair_probability`` must already match the contract side being evaluated.
   Example: if you want to buy ``NO``, pass the fair probability of ``NO``,
   not the fair probability of the underlying event happening.
5. Fees and slippage are modeled as additional entry cost measured in
   probability points:

   ``net_edge = fair_probability - (executable_price + fee_cost + slippage)``

   where ``fee_cost = executable_price * fee_rate``.
"""

from __future__ import annotations

from arb.models.market import KalshiMarketSnapshot, KalshiOrderLevel


class LiquidityError(ValueError):
    """Raised when the displayed order-book size is below the required minimum."""


class BookValidationError(ValueError):
    """Raised when a market order book is internally inconsistent or malformed."""


def select_executable_price(
    market: KalshiMarketSnapshot,
    side: str,
    minimum_size: float = 0.0,
) -> tuple[float, float]:
    """Select the current executable price and available size for a contract side.

    Args:
        market: Normalized Kalshi market snapshot.
        side: Contract side to buy, either ``"yes"`` or ``"no"``.
        minimum_size: Minimum displayed size required to consider the market tradable.

    Returns:
        A ``(price, size)`` tuple using the relevant best ask.

    Raises:
        ValueError: If the side is invalid, the price/size is missing, or
            ``minimum_size`` is negative.
        LiquidityError: If displayed size is below ``minimum_size``.
    """

    normalized_side = side.lower().strip()
    if minimum_size < 0:
        raise ValueError("Minimum size cannot be negative.")

    if normalized_side == "yes":
        price = market.yes_ask
        size = market.yes_ask_size
    elif normalized_side == "no":
        price = market.no_ask
        size = market.no_ask_size
    else:
        raise ValueError("Side must be either 'yes' or 'no'.")

    if price is None or size is None:
        raise ValueError(f"No executable {normalized_side.upper()} ask is available.")
    if size < minimum_size:
        raise LiquidityError(
            f"Available {normalized_side.upper()} ask size {size} is below minimum {minimum_size}."
        )

    return price, size


def calculate_fill_price(
    market: KalshiMarketSnapshot,
    side: str,
    target_size: float,
) -> tuple[float, float]:
    """Calculate a fill-adjusted average entry price for a target size.

    The function walks the ask ladder from best to worst price and computes the
    volume-weighted average price required to fill ``target_size`` contracts.
    """

    if target_size <= 0:
        raise ValueError("Target size must be positive.")

    ask_levels = _get_ask_levels(market, side)
    total_available = sum(level.size for level in ask_levels)
    if total_available < target_size:
        raise LiquidityError(
            f"Available {side.upper()} ask depth {total_available} is below target {target_size}."
        )

    remaining = target_size
    total_cost = 0.0
    for level in ask_levels:
        fill_size = min(level.size, remaining)
        total_cost += fill_size * level.price
        remaining -= fill_size
        if remaining <= 0:
            break

    if remaining > 0:
        raise LiquidityError(f"Unable to fill full target size {target_size}.")

    return total_cost / target_size, total_available


def validate_market_book(
    market: KalshiMarketSnapshot,
    max_dislocation: float = 0.02,
) -> None:
    """Validate that a binary market book is internally sane before pricing.

    Checks:
    - bids/asks and ladder levels stay within [0, 1]
    - sizes are positive
    - ask ladders are sorted best-to-worst
    - best ask is not below same-side best bid
    - explicit best ask is not materially different from the derived ask ladder
    """

    if max_dislocation < 0:
        raise ValueError("Max book dislocation cannot be negative.")

    for value in (market.yes_bid, market.yes_ask, market.no_bid, market.no_ask):
        if value is not None and not 0 <= value <= 1:
            raise BookValidationError("Book price must be between 0 and 1.")

    for size in (market.yes_bid_size, market.yes_ask_size, market.no_bid_size, market.no_ask_size):
        if size is not None and size <= 0:
            raise BookValidationError("Book size must be positive.")

    _validate_ask_ladder(market.yes_ask_levels, "YES")
    _validate_ask_ladder(market.no_ask_levels, "NO")

    if market.yes_bid is not None and market.yes_ask is not None and market.yes_ask < market.yes_bid:
        raise BookValidationError("YES ask is below YES bid.")
    if market.no_bid is not None and market.no_ask is not None and market.no_ask < market.no_bid:
        raise BookValidationError("NO ask is below NO bid.")

    _validate_explicit_vs_ladder(market.yes_ask, market.yes_ask_levels, "YES", max_dislocation)
    _validate_explicit_vs_ladder(market.no_ask, market.no_ask_levels, "NO", max_dislocation)


def calculate_raw_edge(fair_probability: float, executable_price: float) -> float:
    """Calculate raw edge before fees or slippage.

    Formula:
        ``raw_edge = fair_probability - executable_price``

    Args:
        fair_probability: Estimated fair probability for the same contract side.
        executable_price: Current executable market price for that contract side.

    Returns:
        Raw edge in probability points.

    Raises:
        ValueError: If either input is outside ``[0, 1]``.
    """

    _validate_probability(fair_probability, "Fair probability")
    _validate_probability(executable_price, "Executable price")
    return fair_probability - executable_price


def estimate_net_edge(
    fair_probability: float,
    executable_price: float,
    fee_rate: float = 0.0,
    slippage: float = 0.0,
) -> float:
    """Estimate edge after modeled execution costs.

    Formula:
        ``fee_cost = executable_price * fee_rate``
        ``net_edge = fair_probability - executable_price - fee_cost - slippage``

    Args:
        fair_probability: Estimated fair probability for the same contract side.
        executable_price: Current executable market price for that contract side.
        fee_rate: Fraction of the entry price charged as fees. For example,
            ``0.02`` means 2% of the entry price.
        slippage: Additional fixed execution cost in probability points.

    Returns:
        Net edge in probability points after modeled costs.

    Raises:
        ValueError: If probabilities are outside ``[0, 1]`` or if cost inputs are negative.
    """

    raw_edge = calculate_raw_edge(fair_probability, executable_price)

    if fee_rate < 0:
        raise ValueError("Fee rate cannot be negative.")
    if slippage < 0:
        raise ValueError("Slippage cannot be negative.")

    fee_cost = executable_price * fee_rate
    return raw_edge - fee_cost - slippage


def _validate_probability(value: float, name: str) -> None:
    """Validate a probability-like input."""

    if not 0 <= value <= 1:
        raise ValueError(f"{name} must be between 0 and 1.")


def _get_ask_levels(market: KalshiMarketSnapshot, side: str) -> tuple[KalshiOrderLevel, ...]:
    """Return the ask ladder for the requested side."""

    normalized_side = side.lower().strip()
    if normalized_side == "yes":
        levels = market.yes_ask_levels
    elif normalized_side == "no":
        levels = market.no_ask_levels
    else:
        raise ValueError("Side must be either 'yes' or 'no'.")

    if not levels:
        raise ValueError(f"No executable {normalized_side.upper()} ask ladder is available.")
    return levels


def _validate_ask_ladder(levels: tuple[KalshiOrderLevel, ...], side: str) -> None:
    """Ensure an ask ladder is well-formed."""

    previous_price = None
    for level in levels:
        if not 0 <= level.price <= 1:
            raise BookValidationError(f"{side} ask ladder contains an invalid price.")
        if level.size <= 0:
            raise BookValidationError(f"{side} ask ladder contains a non-positive size.")
        if previous_price is not None and level.price < previous_price:
            raise BookValidationError(f"{side} ask ladder is not sorted from best to worst.")
        previous_price = level.price


def _validate_explicit_vs_ladder(
    explicit_ask: float | None,
    levels: tuple[KalshiOrderLevel, ...],
    side: str,
    max_dislocation: float,
) -> None:
    """Ensure the explicit best ask roughly agrees with the ladder best ask."""

    if explicit_ask is None or not levels:
        return

    ladder_best = levels[0].price
    if abs(explicit_ask - ladder_best) > max_dislocation:
        raise BookValidationError(
            f"{side} explicit best ask differs from derived best ask by more than {max_dislocation}."
        )
