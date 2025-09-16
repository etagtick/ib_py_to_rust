# cts_exc_dll.py
from typing import Callable, TypeVar

T = TypeVar('T')


class CtsExc:
    @staticmethod
    def safe_parse(parser_func: Callable[[], T], default: T = None, debug: bool = False) -> T:
        """Generic exception handler for parsing operations"""
        try:
            return parser_func()
        except (ValueError, IndexError, UnicodeDecodeError, AttributeError) as e:
            if debug:
                print(f"[CTS_EXC] Parse error: {type(e).__name__}: {e}")
            return default
        except Exception as e:
            if debug:
                print(f"[CTS_EXC] Unexpected error: {type(e).__name__}: {e}")
            return default

    @staticmethod
    def safe_int(field: bytes, default: int = 0) -> int:
        """Safe integer parsing from bytes field"""
        return CtsExc.safe_parse(lambda: int(field.decode('ascii')), default)

    @staticmethod
    def safe_str(field: bytes, default: str = "") -> str:
        """Safe string parsing from bytes field"""
        return CtsExc.safe_parse(lambda: field.decode('ascii'), default)

    @staticmethod
    def parse_exp_strikes(fields: list[bytes], start_idx: int = 6) -> tuple[list[str], list[str]]:
        """Parse expirations and strikes from fields with safe exception handling"""

        def _parse():
            expirations = []
            strikes = []

            if len(fields) <= start_idx:
                return expirations, strikes

            # Parse expiration count and expirations
            exp_count = int(fields[start_idx].decode('ascii'))
            exp_start = start_idx + 1

            for i in range(exp_count):
                idx = exp_start + i
                if idx >= len(fields):
                    break
                expirations.append(fields[idx].decode('ascii'))

            # Parse strike count and strikes
            strike_count_idx = exp_start + exp_count
            if strike_count_idx >= len(fields):
                return expirations, strikes

            strike_count = int(fields[strike_count_idx].decode('ascii'))
            strike_start = strike_count_idx + 1

            for i in range(strike_count):
                idx = strike_start + i
                if idx >= len(fields):
                    break
                strikes.append(fields[idx].decode('ascii'))

            return expirations, strikes

        return CtsExc.safe_parse(_parse, ([], []))


__all__ = ["CtsExc"]