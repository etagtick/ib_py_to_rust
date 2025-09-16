#/cts/cts_cache
import struct
import asyncio
from typing import List, Tuple
from datetime import datetime as dt, timedelta

from cts.cts_cfg import ROOTS, EXCHANGES, TCLASSES, HISTO_KEY_FORMAT
from cts.cts_api import CtsApi




# === CtsCache: statics for key building, expiry calc, option fetching ===
class CtsCache:
    # --- Public delegates ---
    @staticmethod
    def get_spx_target_expiries() -> List[Tuple[str, int, str]]:
        """Return target expiries (0–6 DTE + monthly)."""
        today = dt.now()
        cache_specs = []
        current_date = today
        dte = 0
        while dte <= 6:
            if current_date.weekday() < 5:
                expiry_mmddy = CtsCache._encode_mmddy(current_date)
                if CtsCache._is_monthly_expiry(current_date):
                    cache_specs.append((f"SPX_{dte}DTE_SPX", expiry_mmddy, "SPX"))
                    cache_specs.append((f"SPX_{dte}DTE_SPXW", expiry_mmddy, "SPXW"))
                else:
                    cache_specs.append((f"SPX_{dte}DTE_SPXW", expiry_mmddy, "SPXW"))
                dte += 1
            current_date += timedelta(days=1)

        for expiry_date in CtsCache._get_monthly_expires(2):
            expiry_mmddy = CtsCache._encode_mmddy(expiry_date)
            cache_specs.append((f"SPX_{expiry_mmddy:05d}_SPX", expiry_mmddy, "SPX"))
        return cache_specs

    # @staticmethod
    # async def fetch_options_multi_gateway(gateway_ports: List[int]) -> List:
    #     """Fetch option contracts across multiple gateways using CDN + IB API."""
    #     print(f"\n=== FETCHING OPTIONS (Multi-Gateway: {gateway_ports}) ===")
    #     cdn_data = await Cdn.get_all_local_symbols()
    #
    #     # Pick the first active OPT entry
    #     root_key = b"SPX\x00"  # adjust if multiple roots active
    #
    #     if not cdn_data or root_key not in cdn_data:
    #         print(f"No options found in CDN for {root_key}")
    #         return []
    #
    #     symbols, spot, cfg = cdn_data[root_key]
    #     print(f"Found {len(symbols)} {root_key} options, spot={spot}")
    #
    #     chunks = CtsCache._split_chunks(symbols, len(gateway_ports))
    #     for port, chunk in zip(gateway_ports, chunks):
    #         print(f"Gateway {port}: {len(chunk)} options")
    #
    #     tasks = [
    #         CtsCache._process_options_on_gateway(port, chunk, idx)
    #         for idx, (port, chunk) in enumerate(zip(gateway_ports, chunks)) if chunk
    #     ]
    #     results = await asyncio.gather(*tasks, return_exceptions=True)
    #     all_contracts = []
    #     for port, res in zip(gateway_ports, results):
    #         tmp = res
    #         if isinstance(res, Exception):
    #             print(f"Gateway {port} failed: {res}")
    #         else:
    #             all_contracts.extend(tmp)
    #             print(f"Gateway {port} retrieved {len(tmp)} contracts")
    #     return all_contracts

    # --- Private helpers ---
    def encode_mmddy(date: str) -> int:
        """Convert YYYYMMDD int to mmddy (fits in 16-bit)."""
        s = str(date)
        yyyy, mm, dd = int(s[:4]), int(s[4:6]), int(s[6:8])
        return int(f"{mm:02d}{dd:02d}{str(yyyy)[3]}")

    #@staticmethod
    #def _encode_mmddy(date: dt) -> int:
    #    return int(f"{date.month:02d}{date.day:02d}{str(date.year)[3]}")

    @staticmethod
    def _get_monthly_expires(count: int) -> List[dt]:
        today = dt.now()
        expires = []
        current_month, current_year = today.month, today.year
        for _ in range(count):
            first_day = dt(current_year, current_month, 1)
            first_friday = first_day + timedelta(days=(4 - first_day.weekday() + 7) % 7)
            third_friday = first_friday + timedelta(days=14)
            if third_friday.date() <= today.date():
                current_month += 1
                if current_month > 12:
                    current_month, current_year = 1, current_year + 1
                continue
            expires.append(third_friday)
            current_month += 1
            if current_month > 12:
                current_month, current_year = 1, current_year + 1
        return expires

    @staticmethod
    def _is_monthly_expiry(date: dt) -> bool:
        first_day = dt(date.year, date.month, 1)
        first_friday = first_day + timedelta(days=(4 - first_day.weekday() + 7) % 7)
        third_friday = first_friday + timedelta(days=14)
        return date.date() == third_friday.date()

    @staticmethod
    def _split_chunks(seq, n):
        k, m = divmod(len(seq), n)
        return [seq[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]

    @staticmethod
    async def _process_options_on_gateway(port: int, options: List[str], gateway_idx: int) -> List:
        api = CtsApi(slot=gateway_idx + 1)
        api.tws.port = port
        contracts = []
        try:
            await api.tws.connect_async()
            for i, sym in enumerate(options):
                try:
                    result = await api.req_cboe_opt(sym)
                    if result:
                        contracts.append(result)
                    if (i + 1) % 100 == 0:
                        print(f"[{port}] Progress {i+1}/{len(options)}")
                    await asyncio.sleep(0.2)
                except Exception as e:
                    print(f"[{port}] Error {sym}: {e}")
            return contracts
        finally:
            await api.tws.close_async()
            print(f"Gateway {port} disconnected")

    @staticmethod
    def parse_cdn_symbol_to_key(sym: str, cfg: dict):
        """
        Parse a local symbol according to config from cts_cfg (no hardcoding).
        Returns dict with fields ready for CtsCache.gen_key().
        """
        root = cfg["root"]
        tclass = cfg["tc"]
        exchange = cfg["xch"]
        step = cfg["step"]

        # expiry part
        expiry_str = sym[-15:-9]  # e.g. '250919'
        expiry_date = dt.strptime(expiry_str, "%y%m%d")

        # normalize: SPX monthly (AM-settled) -> shift -1 day at IB
        if tclass == b"SPX\x00":
            expiry_date -= timedelta(days=1)

        # pack expiry as mmddy (MMDD + last digit of year)
        expiry_mmddy = int(f"{expiry_date.month:02d}{expiry_date.day:02d}{str(expiry_date.year)[3]}")

        # right (C or P)
        right_char = sym[-9]
        right = 1 if right_char == "C" else 2

        # strike (/1000 from trailing digits)
        strike_str = sym[-8:]
        strike = float(strike_str) / 1000.0

        return {
            "root": root,
            "expiry_mmddy": expiry_mmddy,  # fixed key
            "strike": strike,
            "right": right,
            "exchange": exchange,
            "tclass": tclass,
            "step": step,
        }

    @staticmethod
    def _get_index_safe(array: List[bytes], value: bytes) -> int:
        try:
            return array.index(value)
        except ValueError:
            array.append(value)
            return len(array) - 1

    @staticmethod
    def _encode_strike(strike: float, step: float) -> int:
        return 0 if strike == 0.0 else int(strike / step)

    @staticmethod
    def gen_key(root: bytes, expiry_mmddy: int, strike: float,
                right: int, exchange: bytes, tclass: bytes,
                step: float) -> bytes:
        """Generate the unique 8-byte key for a contract (unified for CDN & IB)."""
        root_idx = CtsCache._get_index_safe(ROOTS, root)
        exchange_idx = CtsCache._get_index_safe(EXCHANGES, exchange)
        tclass_idx = CtsCache._get_index_safe(TCLASSES, tclass)
        encoded_strike = CtsCache._encode_strike(strike, step)

        return struct.pack(
            HISTO_KEY_FORMAT,
            root_idx,
            exchange_idx,
            expiry_mmddy,
            encoded_strike,
            right,
            tclass_idx,
        )
