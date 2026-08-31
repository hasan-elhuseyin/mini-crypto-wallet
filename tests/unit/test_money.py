"""Money must never touch a float."""

from decimal import Decimal

import pytest
from mcw_common.assets import get_asset
from mcw_common.money import AmountError, format_amount, parse_amount

pytestmark = pytest.mark.unit

USDT = get_asset("USDT")


@pytest.mark.parametrize(
    ("text", "expected_units"),
    [
        ("1000.000000", 1_000_000_000),
        ("250.000000", 250_000_000),
        ("250", 250_000_000),
        ("0.000001", 1),
        ("0.1", 100_000),
        ("999999999.999999", 999_999_999_999_999),
    ],
)
def test_parse_amount_exact(text, expected_units):
    assert parse_amount(text, USDT.decimals) == expected_units


@pytest.mark.parametrize(
    ("units", "expected"),
    [
        (1_000_000_000, "1000.000000"),
        (750_000_000, "750.000000"),
        (1, "0.000001"),
        (0, "0.000000"),
        (-250_000_000, "-250.000000"),
    ],
)
def test_format_amount(units, expected):
    assert format_amount(units, USDT.decimals) == expected


def test_round_trip_is_lossless():
    for text in ("1000.000000", "0.000001", "123456.789012"):
        assert format_amount(parse_amount(text, 6), 6) == text


def test_float_input_is_rejected():
    # The whole point: 250.1 as a float is not 250.1.
    with pytest.raises(AmountError, match="float"):
        parse_amount(250.1, USDT.decimals)


def test_boolean_is_not_an_amount():
    with pytest.raises(AmountError):
        parse_amount(True, USDT.decimals)


@pytest.mark.parametrize("bad", ["-1", "-0.5"])
def test_negative_amount_is_rejected(bad):
    with pytest.raises(AmountError, match="negative"):
        parse_amount(bad, USDT.decimals)


def test_zero_is_rejected_by_default():
    with pytest.raises(AmountError, match="greater than zero"):
        parse_amount("0", USDT.decimals)
    assert parse_amount("0", USDT.decimals, allow_zero=True) == 0


def test_excess_precision_is_rejected_not_truncated():
    # 0.0000001 USDT cannot be represented; silently rounding it would lose money.
    with pytest.raises(AmountError, match="decimal places"):
        parse_amount("0.0000001", USDT.decimals)


@pytest.mark.parametrize("bad", ["1e6", "1E6", "abc", "", "  ", "1.2.3"])
def test_malformed_input_is_rejected(bad):
    with pytest.raises(AmountError):
        parse_amount(bad, USDT.decimals)


def test_decimal_input_is_accepted():
    assert parse_amount(Decimal("250.000000"), USDT.decimals) == 250_000_000


def test_format_rejects_non_int_units():
    with pytest.raises(AmountError):
        format_amount(1.5, USDT.decimals)  # type: ignore[arg-type]
