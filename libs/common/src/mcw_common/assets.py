"""Supported assets / networks registry.

Kept deliberately static: an asset listing is a deployment decision, not
runtime data. Adding a chain means adding an entry here plus a chain adapter.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["Asset", "ASSETS", "get_asset", "is_supported", "UnsupportedAssetError"]


class UnsupportedAssetError(KeyError):
    pass


@dataclass(frozen=True, slots=True)
class Asset:
    symbol: str
    decimals: int
    network: str
    standard: str
    #: Token contract. For the simulated chain this is a stable fake address so
    #: that log filtering behaves exactly like it does against a real node.
    contract_address: str
    confirmations_required: int
    #: Depth after which we stop watching for reorgs and treat history as final.
    finality_depth: int


ASSETS: dict[str, Asset] = {
    "USDT": Asset(
        symbol="USDT",
        decimals=6,
        network="BSC",
        standard="BEP-20",
        contract_address="0x55d398326f99059fF775485246999027B3197955",
        confirmations_required=3,
        finality_depth=15,
    ),
}


def get_asset(symbol: str) -> Asset:
    try:
        return ASSETS[symbol.upper()]
    except (KeyError, AttributeError) as exc:
        raise UnsupportedAssetError(symbol) from exc


def is_supported(symbol: str) -> bool:
    return isinstance(symbol, str) and symbol.upper() in ASSETS
