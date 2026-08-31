"""Money handling.

Hard rule for this platform: **no floats, ever**.

Every amount is carried in the asset's *smallest unit* as a Python ``int``
(USDT has 6 decimals, so 1 USDT == 1_000_000 units). ``Decimal`` is used only
at the parsing/formatting boundary, never for arithmetic on stored values.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, localcontext

__all__ = ["AmountError", "parse_amount", "format_amount", "to_units", "from_units"]

# Enough precision for 10^30 tokens with 18 decimals.
_PRECISION = 80


class AmountError(ValueError):
    """Raised when a user supplied amount cannot be represented exactly."""


def parse_amount(value: str | int | Decimal, decimals: int, *, allow_zero: bool = False) -> int:
    """Parse a human amount ("250.000000") into smallest units (250000000).

    Rejects floats outright, rejects NaN/Inf, rejects scientific notation and
    rejects values with more precision than the asset supports (silently
    truncating a user's money is worse than a 422).
    """
    if isinstance(value, bool):  # bool is an int subclass; never a valid amount
        raise AmountError("amount must not be a boolean")
    if isinstance(value, float):
        raise AmountError("float amounts are not accepted; use a decimal string")
    if isinstance(value, int):
        dec = Decimal(value)
    else:
        text = str(value).strip()
        if not text:
            raise AmountError("amount must not be empty")
        if "e" in text.lower():
            raise AmountError("scientific notation is not accepted")
        try:
            dec = Decimal(text)
        except InvalidOperation as exc:
            raise AmountError(f"'{value}' is not a valid decimal amount") from exc

    if not dec.is_finite():
        raise AmountError("amount must be finite")
    if dec < 0:
        raise AmountError("amount must not be negative")
    if dec == 0 and not allow_zero:
        raise AmountError("amount must be greater than zero")

    with localcontext() as ctx:
        ctx.prec = _PRECISION
        scaled = dec.scaleb(decimals)
        if scaled != scaled.to_integral_value():
            raise AmountError(f"amount has more than {decimals} decimal places")
        return int(scaled.to_integral_value())


def format_amount(units: int, decimals: int) -> str:
    """Render smallest units as a fixed-precision decimal string.

    Always returns exactly ``decimals`` fractional digits so that clients can
    compare strings without re-normalising. Handles negatives (ledger entries
    are signed).
    """
    if isinstance(units, bool) or not isinstance(units, int):
        raise AmountError("units must be an int in the asset's smallest unit")
    sign = "-" if units < 0 else ""
    magnitude = abs(units)
    if decimals == 0:
        return f"{sign}{magnitude}"
    whole, frac = divmod(magnitude, 10**decimals)
    return f"{sign}{whole}.{frac:0{decimals}d}"


# Aliases that read better at call sites dealing with a known asset.
to_units = parse_amount
from_units = format_amount
