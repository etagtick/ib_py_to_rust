#!/usr/bin/env python3
import struct
from typing import List
# from datetime import datetime as dt, timedelta

from cts.cts_cfg import ROOTS, EXCHANGES, TCLASSES, HISTO_KEY_FORMAT


class CtsCache:
    """
    Static helpers for cache key generation, option expiry handling,
    and CDN/IB interop.
    """

    # --- Expiry handling ---
    # @staticmethod
    # def encode_mmddy(date: str | int) -> int:
    #     """
    #     Convert expiry (YYYYMMDD int or 'YYMMDD' string) to mmddy (16-bit).
    #     FUT can pass 'YYMM' (=> MM00Y), PERM passes 0.
    #     """
    #     if date in (0, "0", None):
    #         return 0
    #     s = str(date)
    #     if len(s) == 6:  # YYMMDD
    #         y, m, d = int(s[:2]), int(s[2:4]), int(s[4:6])
    #         return int(f"{m:02d}{d:02d}{y%10}")
    #     elif len(s) == 4:  # YYMM
    #         y, m = int(s[:2]), int(s[2:4])
    #         return int(f"{m:02d}00{y%10}")
    #     elif len(s) == 8:  # YYYYMMDD
    #         yyyy, mm, dd = int(s[:4]), int(s[4:6]), int(s[6:8])
    #         return int(f"{mm:02d}{dd:02d}{yyyy%10}")
    #     raise ValueError(f"Unsupported expiry format: {s}")

    # @staticmethod
    # def _get_monthly_expires(count: int) -> List[dt]:
    #     today = dt.now()
    #     expires = []
    #     current_month, current_year = today.month, today.year
    #     for _ in range(count):
    #         first_day = dt(current_year, current_month, 1)
    #         first_friday = first_day + timedelta(days=(4 - first_day.weekday() + 7) % 7)
    #         third_friday = first_friday + timedelta(days=14)
    #         if third_friday.date() > today.date():
    #             expires.append(third_friday)
    #         current_month += 1
    #         if current_month > 12:
    #             current_month, current_year = 1, current_year + 1
    #     return expires
    #
    # @staticmethod
    # def _is_monthly_expiry(date: dt) -> bool:
    #     first_day = dt(date.year, date.month, 1)
    #     first_friday = first_day + timedelta(days=(4 - first_day.weekday() + 7) % 7)
    #     third_friday = first_friday + timedelta(days=14)
    #     return date.date() == third_friday.date()

    # --- Parsing ---
    @staticmethod
    def parse_cdn_symbol_to_key(sym: str, cfg: dict) -> bytes:
        """
        Parse a CDN local symbol into fields suitable for gen_key().
        """
        root = cfg["root"]
        exchange = cfg["xch"]
        step = cfg["step"]
        tc = b'sym[:-15]\x00'
        expiry_str = sym[-15:-9]  # e.g. '250919'
        #expiry_date = dt.strptime(expiry_str, "%y%m%d")
        #if tclass == b"SPX\x00":  # adjust AM-settled
        #    expiry_date -= timedelta(days=1)

        right = 1 if sym[-9] == "C" else 2
        strike = float(sym[-8:]) / 1000.0

        return CtsCache.gen_key(root, expiry_str, strike, right, exchange, tc, step)


    # --- Encoding indexes ---
    # @staticmethod
    # def _get_index_safe(array: List[bytes], value: bytes) -> int:
    #     try:
    #         return array.index(value)
    #     except ValueError:
    #         array.append(value)
    #         return len(array) - 1
    #
    # @staticmethod
    # def _encode_strike(strike: float, step: float) -> int:
    #     return 0 if strike == 0 else int(strike / step)

    # --- Key generation ---
    @staticmethod
    def gen_key(root: bytes, expiry: str| int, strike: float, right: int, exchange: bytes, tc: bytes, step: float) -> bytes:
        """Generate the unique 8-byte key."""
        def encode_mmddy(date: str | int) -> int:
            """
            Convert expiry (YYYYMMDD int or 'YYMMDD' string) to mmddy (16-bit).
            FUT can pass 'YYMM' (=> MM00Y), PERM passes 0.
            """
            if date in (0, "0", None,''):
                return 0
            s = str(date)
            if len(s) == 6:  # YYMMDD
                y, m, d = int(s[:2]), int(s[2:4]), int(s[4:6])
                return int(f"{m:02d}{d:02d}{y % 10}")
            elif len(s) == 4:  # YYMM
                y, m = int(s[:2]), int(s[2:4])
                return int(f"{m:02d}00{y % 10}")
            elif len(s) == 8:  # YYYYMMDD
                yyyy, mm, dd = int(s[:4]), int(s[4:6]), int(s[6:8])
                return int(f"{mm:02d}{dd:02d}{yyyy % 10}")
            raise ValueError(f"Unsupported expiry format: {s}")
        def _get_index_safe(array: List[bytes], value: bytes) -> int:
            try:
                return array.index(value)
            except ValueError:
                array.append(value)
                return len(array) - 1
        def _encode_strike(_strike: float, _step: float) -> int:
            return 0 if _strike == 0 else int(_strike / _step)

        root_idx = _get_index_safe(ROOTS, root)
        exchange_idx = _get_index_safe(EXCHANGES, exchange)
        tc_idx = _get_index_safe(TCLASSES, tc)
        encoded_strike = _encode_strike(strike, step)
        expiry_mmddy= encode_mmddy(expiry)

        return struct.pack(
            HISTO_KEY_FORMAT,root_idx, exchange_idx, expiry_mmddy, encoded_strike, right, tc_idx)

    @staticmethod
    def gen_key2(cfg: dict, expiry: str='', strike: float=0.0, right: int=0) -> bytes:
        return CtsCache.gen_key(cfg['root'], expiry, strike, right, cfg['xch'], cfg['tc'], cfg.get('step',1))
