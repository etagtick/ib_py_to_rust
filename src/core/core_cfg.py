# core_cfg.py
from __future__ import annotations

from typing import Final

"""
Global configuration for protocol versions and field indexes.
Keeps IBKR protocol drift isolated here.
"""
"""
core_cfg.py — Core configuration & constants
Strict, bytes-only core for IBKR wire I/O.
"""

# IBKR protocol handshake constants
IBKR_MAGIC: bytes = b"API\x00"  # literal "API\0"
IBKR_MIN_CLIENT_VERSION: int = 157
IBKR_MAX_CLIENT_VERSION: int = 178

# Connection defaults
DEFAULT_CONNECT_TIMEOUT_SEC: int = 10
MAX_FRAME_SIZE: int = 16 * 1024 * 1024  # 16 MiB hard ceiling to avoid bad frames

INFO_ERROR_CODES = {2104, 2107, 2158}

# ---- Business → clientId ranges (inclusive) ----
BUSINESS_RANGES: Final[dict[str, tuple[int, int]]] = {
    "cts": (1000, 1999),
    "mkt": (2000, 2999),
    "hst": (3000, 3999),
    "ord": (4000, 4999),
    "acc": (5000, 5999),
}

class ClientIdError(Exception):
    __slots__ = ("message",)
    def __init__(self, message: str) -> None:
        self.message = message
    def __str__(self) -> str:
        return self.message

def client_id(business: str, slot: int) -> int:
    """
    Deterministically compute the clientId for an app/service:
    - business: one of BUSINESS_RANGES keys (case-insensitive)
    - slot: non-negative integer offset in the business range
    Returns the clientId (int). Raises ClientIdError on invalid input.
    """
    if not isinstance(business, str) or not business:
        raise ClientIdError("client_id: business must be a non-empty string")
    b = business.lower()
    rng = BUSINESS_RANGES.get(b)
    if rng is None:
        raise ClientIdError(f"client_id: unknown business '{business}'")
    if not isinstance(slot, int):
        raise ClientIdError("client_id: slot must be int")
    if slot < 0:
        raise ClientIdError("client_id: slot must be >= 0")
    lo, hi = rng
    cid = lo + slot
    if cid > hi:
        raise ClientIdError(f"client_id: slot {slot} exceeds range {lo}-{hi} for '{business}'")
    return cid

# -----------------------------------------------------------------------------
# Versioned schema (STRICT). For bytes-only selective parsers.
# We DO NOT bake in global absolute field indices that can drift across builds.
# Instead, we expose:
#   - message tags or tickTypes to match
#   - the exact fields we expect to extract (in order), so scanners can walk the
#     NUL-separated payload once and slice only what matters.
# -----------------------------------------------------------------------------

CORE_CFG: Final[dict] = {
    # Negotiated at handshake
    "MinClientVersion": IBKR_MIN_CLIENT_VERSION,
    "MaxClientVersion": IBKR_MAX_CLIENT_VERSION,

    "layouts": {
        178: {  # stable as of Sep 2025
            # ---------- CTS ----------
            "contractDetails": {
                # NUL-field indices *within* the contractDetails payload.
                # (Used where you already rely on fixed indices.)
                "reqId": 1,
                "conId": 12,
                "expiry": 4,
            },

            # Security Definition Option Parameters (secDefOptParams)
            # We expose the fields we will extract (in order). Your scanner will:
            # - match the message by tag (e.g., b"84" or whatever your wire tag is),
            # - then walk the payload and extract these fields, in order,
            #   decoding strikes/expirations sets per your reader.
            "secDefOptParams": {
                "fields": [
                    "reqId",
                    "exchange",
                    "underlyingConId",
                    "tradingClass",
                    "multiplier",
                    "expirations",  # set/list; parse as a NUL-delimited list or IB-style block as you prefer
                    "strikes",      # set/list
                ]
            },

            # ---------- MARKET DATA ----------
            "marketData": {
                # A "trade_msg" is your composite: tickPrice(4), tickSize(5,8), timestamp(49)
                # Scanners should only act on these tickTypes.
                "trade_msg": {
                    "tickTypes": {
                        "lastPrice": 4,     # tickPrice(4)
                        "lastSize": 5,      # tickSize(5)
                        "volume":   8,      # tickSize(8)
                        "lastTime": 49,     # tickByTick last timestamp or RTVolume timestamp depending on source
                    }
                },

                # Option computation ticks (we handle only bid/ask/model, not 'last')
                # Scanner should react only to these tickTypes and then extract the full set you care about.
                # Field names below are the canonical IB wrapper names; keep them in this order.
                "tickOptionComputation": {
                    "tickTypes": {
                        "bid":   10,
                        "ask":   11,
                        "model": 13,
                    },
                    "fields": [
                        "impliedVol",
                        "delta",
                        "optPrice",
                        "pvDividend",
                        "gamma",
                        "vega",
                        "theta",
                        "underPrice",
                    ]
                },
            },

            # ---------- ORDERS ----------
            # We expose only what you asked for. Your scanner will:
            # - match the message by tag (e.g., orderStatus),
            # - read these fields in order from the payload.
            "orderStatus": {
                "fields": [
                    "clientId",
                    "orderRef",
                    "totalQuantity",
                    "limitPrice",
                    "filled",          # qty executed
                    "remaining",       # qty pending
                    "avgFillPrice",    # VWAP of executed part (IB average fill price)
                    "orderStatus",     # e.g., Submitted, Filled, Cancelled...
                ]
            },
        }
    }
}


def get_layout(server_version: int, message: str) -> dict:
    """
    Return the strict layout mapping for a given server_version and message.
    Raises KeyError if not defined. No fallbacks by design.
    """
    try:
        return CORE_CFG["layouts"][server_version][message]
    except KeyError as e:
        raise KeyError(
            f"layout not defined for serverVersion={server_version}, message='{message}'"
        ) from e


__all__ = [
    "IBKR_MAGIC",
    "IBKR_MIN_CLIENT_VERSION",
    "IBKR_MAX_CLIENT_VERSION",
    "DEFAULT_CONNECT_TIMEOUT_SEC",
    "MAX_FRAME_SIZE",
    "BUSINESS_RANGES",
    "ClientIdError",
    "client_id",
    "CORE_CFG",
    "get_layout",
    "INFO_ERROR_CODES",
]