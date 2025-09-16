import math
import struct
from typing import Dict, Optional

from cts.cts_cache import CtsCache, HISTO_KEY_FORMAT
from cts.cts_cfg import ROOTS, EXCHANGES, TCLASSES
from cts.cts_hst_cache import HistoCache


class SpxHotCache:
    def __init__(self, cache_name: str, expiry_mmddy: int, trading_class: str,
                 underlying_price: float, step: float = 5.0):
        self.cache_name = cache_name
        self.expiry = expiry_mmddy
        self.trading_class = trading_class
        self.underlying = underlying_price
        self.step = step
        self.cache = [b''] * 256
        self.base_strike = math.floor(underlying_price / step) * step

    def get_index_from_strike(self, strike: float, is_put: bool) -> int:
        offset = int((strike - self.base_strike) / self.step)
        return (128 + offset + 127) if is_put else (offset + 127)

    def set_conid(self, strike: float, is_put: bool, conid_binary: bytes):
        idx = self.get_index_from_strike(strike, is_put)
        if not (0 <= idx < 256):
            raise ValueError(f"Strike index {idx} out of range")
        if len(conid_binary) > 18:
            raise ValueError(f"ConId binary too long: {len(conid_binary)} bytes")
        self.cache[idx] = conid_binary

    def get_conid(self, strike: float, is_put: bool) -> bytes:
        idx = self.get_index_from_strike(strike, is_put)
        return self.cache[idx] if 0 <= idx < 256 else b''

    # --- Public orchestration ---
    @staticmethod
    def build_from_histo(histo_cache: HistoCache, underlying_price: float) -> Dict[str, "SpxHotCache"]:
        specs = CtsCache.get_spx_target_expiries()
        return {
            name: SpxHotCache._build_one_cache(histo_cache, name, expiry, tclass, underlying_price)
            for name, expiry, tclass in specs
        }

    @staticmethod
    def get_for_lookup(hot_caches: Dict[str, "SpxHotCache"],
                       dte: int, trading_class: str, target_date: int = None) -> Optional["SpxHotCache"]:
        return hot_caches.get(SpxHotCache._make_cache_name(dte, trading_class, target_date))

    # --- Private helpers ---
    @staticmethod
    def _build_one_cache(histo_cache: HistoCache, cache_name: str,
                         expiry_mmddy: int, trading_class: str,
                         underlying_price: float) -> "SpxHotCache":
        hot_cache = SpxHotCache(cache_name, expiry_mmddy, trading_class, underlying_price)
        for key, conid_binary in histo_cache.records.items():
            SpxHotCache._try_add_to_cache(hot_cache, key, conid_binary, expiry_mmddy, trading_class)
        return hot_cache

    @staticmethod
    def _try_add_to_cache(hot_cache: "SpxHotCache", key: bytes,
                          conid_binary: bytes, expiry_mmddy: int,
                          trading_class: str):
        spx_root_idx = CtsCache.get_index_safe(ROOTS, b'SPX\x00')
        cboe_exchange_idx = CtsCache.get_index_safe(EXCHANGES, b'CBOE\x00')
        tclass_idx = CtsCache.get_index_safe(TCLASSES, trading_class.encode() + b"\x00")

        root_idx, exchange_idx, key_expiry, encoded_strike, right, tclass, _ = struct.unpack(HISTO_KEY_FORMAT, key)
        if (root_idx == spx_root_idx and exchange_idx == cboe_exchange_idx and
            tclass == tclass_idx and key_expiry == expiry_mmddy and right in (1, 2)):
            strike = encoded_strike * hot_cache.step
            is_put = (right == 2)
            hot_cache.set_conid(strike, is_put, conid_binary)

    @staticmethod
    def _make_cache_name(dte: int, trading_class: str, target_date: Optional[int]) -> str:
        if dte <= 6:
            return f"SPX_{dte}DTE_{trading_class}"
        if target_date and trading_class == "SPX":
            return f"SPX_{target_date:05d}_SPX"
        return ""
