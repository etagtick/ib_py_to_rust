# cts_dll_old.py - Contract service using generic core components
from typing import Tuple, Optional
from cts.cts_cfg import root, exch, CUR
from core.core_cfg import get_layout
from core_old.core_enc_dll import ascii_field, build_chunks
from core_old.core_dec_dll import find_field_offsets, parse_ascii_int, debug_payload
from core_old.core_msg_dll import send_msg, recv_until, parse_error_frame, FrameHandler

# Contract-specific pre-encoded data (startup cost, runtime gain)
print("[CTS] Pre-encoding contract data...")

# Pre-encoded symbols, exchanges, currencies with NUL terminators
_SYM_BYTES = [rt[0].encode('ascii') + b'\x00' for rt in root]
_XCH_BYTES = [x.encode('ascii') + b'\x00' for x in exch]
_CUR_BYTES = [c.encode('ascii') + b'\x00' for c in CUR]

print(f"[CTS] Pre-encoded {len(_SYM_BYTES)} symbols, {len(_XCH_BYTES)} exchanges, {len(_CUR_BYTES)} currencies")

# Contract-specific static chunks (version 10)
_STATIC_CHUNKS = {
    'STK': {
        'prefix': b"9\x0010\x00",  # msgId + version
        'mid1': b"\x00",  # conId(empty) + symbol(variable)
        'mid2': b"STK\x00\x00",  # secType + lastTradingDay(empty)
        'mid3': b"0\x00\x00\x00",  # strike + right(empty) + multiplier(empty)
        'mid4': b"\x00",  # exchange(variable) + primaryExchange(empty)
        'mid5': b"\x00\x00",  # currency(variable) + localSymbol(empty)
        'suffix': b"\x000\x00\x00\x00",  # tradingClass + includeExpired + secIdType + secId
    },
    'IND': {
        'prefix': b"9\x0010\x00",
        'mid1': b"\x00",
        'mid2': b"IND\x00\x00",
        'mid3': b"0\x00\x00\x00",
        'mid4': b"\x00",
        'mid5': b"\x00\x00",
        'suffix': b"\x000\x00\x00\x00",
    },
    'FX': {
        'prefix': b"9\x0010\x00",
        'mid1': b"\x00",
        'mid2': b"CASH\x00\x00",
        'mid3': b"0\x00\x00\x00",
        'mid4': b"\x00",
        'mid5': b"\x00\x00",
        'suffix': b"\x000\x00\x00\x00",
    },
    'FUT': {
        'prefix': b"9\x0010\x00",
        'mid1': b"\x00",
        'mid2': b"FUT\x00",  # secType + expiry(variable)
        'mid3': b"\x000\x00\x00\x00",  # expiry + strike + right + multiplier
        'mid4': b"\x00",
        'mid5': b"\x00\x00",
        'suffix': b"\x001\x00\x00\x00",  # includeExpired=1 for futures
    },
    'OPT': {
        'prefix': b"9\x0010\x00",
        'mid1': b"\x00\x00",  # conId + symbol(empty)
        'mid2': b"OPT\x00\x00",  # secType + lastTradingDay(empty)
        'mid3': b"0\x00\x00\x00",  # strike + right + multiplier
        'mid4': b"\x00",  # exchange(variable) + primaryExchange(empty)
        'mid5': b"USD\x00",  # currency(hardcoded) + localSymbol(variable)
        'suffix': b"\x00\x000\x00\x00\x00",  # localSymbol + tradingClass + includeExpired + secIdType + secId
    }
}

print("[CTS] Pre-built static chunks for all contract types")


class CtsFrameHandler(FrameHandler):
    """Contract-specific frame processing (SYNCHRONOUS)"""

    @staticmethod
    def process_cd_frame(payload: bytes, want_req_id: bytes, layout: dict) -> tuple[
        bool, tuple[Optional[int], Optional[bytes]]]:
        """Process contractDetails frame. Returns (done, (conid, expiry))"""
        tag = payload[:payload.find(b'\x00')] if b'\x00' in payload else payload

        if tag == b"4":  # Error
            req_id, code, message = parse_error_frame(payload)
            if req_id == want_req_id.decode('ascii', 'replace') or req_id == "-1":
                if code and not FrameHandler.is_info_error_code(code):
                    debug_payload(payload, "ERROR")
                    raise RuntimeError(f"IB error {code}: {message}")
            return False, (None, None)

        elif tag == b"10":  # contractDetails
            debug_payload(payload, "CONTRACT_DETAILS")

            # Check reqId match
            if not FrameHandler.check_req_id_match(payload, want_req_id, layout["reqId"]):
                return False, (None, None)

            # Extract conId and expiry
            max_idx = max(layout["reqId"], layout["conId"], layout["expiry"])
            want_fields = {layout["reqId"], layout["conId"], layout["expiry"]}
            offsets = find_field_offsets(payload, want_fields, max_idx)

            conid = None
            expiry = None

            if layout["conId"] in offsets:
                start, end = offsets[layout["conId"]]
                conid = parse_ascii_int(payload, start, end)

            if layout["expiry"] in offsets:
                start, end = offsets[layout["expiry"]]
                expiry = payload[start:end]

            print(f"[CTS] Extracted: conid={conid}, expiry={expiry}")
            return False, (conid, expiry)

        elif tag == b"52":  # contractDetailsEnd
            debug_payload(payload, "CONTRACT_DETAILS_END")
            return True, (None, None)  # Signal completion

        return False, (None, None)


class CtsDll:
    """Contract service - minimal signatures with indexed parameters"""

    @staticmethod
    def _build_stk_pay(req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> bytes:
        """Build STK payload from pre-built chunks and indexed data (SYNCHRONOUS)"""
        chunks = _STATIC_CHUNKS['STK']
        rid_field = ascii_field(req_id)

        return build_chunks([
            chunks['prefix'], rid_field, chunks['mid1'], _SYM_BYTES[sym_idx],
            chunks['mid2'], chunks['mid3'], _XCH_BYTES[xch_idx],
            chunks['mid4'], _CUR_BYTES[cur_idx], chunks['mid5'], chunks['suffix']
        ])

    @staticmethod
    def _build_ind_pay(req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> bytes:
        """Build IND payload (SYNCHRONOUS)"""
        chunks = _STATIC_CHUNKS['IND']
        rid_field = ascii_field(req_id)

        return build_chunks([
            chunks['prefix'], rid_field, chunks['mid1'], _SYM_BYTES[sym_idx],
            chunks['mid2'], chunks['mid3'], _XCH_BYTES[xch_idx],
            chunks['mid4'], _CUR_BYTES[cur_idx], chunks['mid5'], chunks['suffix']
        ])

    @staticmethod
    def _build_fx_pay(req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> bytes:
        """Build FX payload (SYNCHRONOUS)"""
        chunks = _STATIC_CHUNKS['FX']
        rid_field = ascii_field(req_id)

        return build_chunks([
            chunks['prefix'], rid_field, chunks['mid1'], _SYM_BYTES[sym_idx],
            chunks['mid2'], chunks['mid3'], _XCH_BYTES[xch_idx],
            chunks['mid4'], _CUR_BYTES[cur_idx], chunks['mid5'], chunks['suffix']
        ])

    @staticmethod
    def _build_fut_pay(req_id: int, sym_idx: int, exp_yyyymm: int, xch_idx: int, cur_idx: int) -> bytes:
        """Build FUT payload (SYNCHRONOUS)"""
        chunks = _STATIC_CHUNKS['FUT']
        rid_field = ascii_field(req_id)
        exp_field = ascii_field(exp_yyyymm)

        return build_chunks([
            chunks['prefix'], rid_field, chunks['mid1'], _SYM_BYTES[sym_idx],
            chunks['mid2'], exp_field, chunks['mid3'], _XCH_BYTES[xch_idx],
            chunks['mid4'], _CUR_BYTES[cur_idx], chunks['mid5'], chunks['suffix']
        ])

    @staticmethod
    def _build_opt_pay(req_id: int, loc_sym: bytes, xch_idx: int) -> bytes:
        """Build OPT payload (SYNCHRONOUS)"""
        chunks = _STATIC_CHUNKS['OPT']
        rid_field = ascii_field(req_id)
        loc_field = loc_sym + b'\x00'

        return build_chunks([
            chunks['prefix'], rid_field, chunks['mid1'], chunks['mid2'],
            chunks['mid3'], _XCH_BYTES[xch_idx], chunks['mid4'],
            chunks['mid5'], loc_field, chunks['suffix']
        ])

    # Public interface - async only for the recv part
    @staticmethod
    async def req_stk(prt, req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> Tuple[
        Optional[int], Optional[bytes]]:
        """Request stock contract (ASYNC - waits for response)"""
        # Build and send (synchronous)
        payload = CtsDll._build_stk_pay(req_id, sym_idx, xch_idx, cur_idx)
        send_msg(prt, payload, debug=True)

        # Receive (asynchronous)
        want_req_id = ascii_field(req_id)[:-1]  # Remove NUL terminator for comparison
        layout = get_layout(prt.server_version, "contractDetails")

        # Keep receiving until we get contractDetailsEnd
        last_result = (None, None)
        try:
            while True:
                done, result = await recv_until(prt, 10.0,
                                                lambda fp: CtsFrameHandler.process_cd_frame(fp, want_req_id, layout))
                if result != (None, None):
                    last_result = result
                # recv_until returns when done=True (contractDetailsEnd), so we're finished
                return last_result
        except Exception:
            return last_result

    @staticmethod
    async def req_ind(prt, req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> Tuple[
        Optional[int], Optional[bytes]]:
        """Request index contract (ASYNC - waits for response)"""
        payload = CtsDll._build_ind_pay(req_id, sym_idx, xch_idx, cur_idx)
        send_msg(prt, payload, debug=True)

        want_req_id = ascii_field(req_id)[:-1]
        layout = get_layout(prt.server_version, "contractDetails")

        last_result = (None, None)
        try:
            while True:
                done, result = await recv_until(prt, 10.0,
                                                lambda fp: CtsFrameHandler.process_cd_frame(fp, want_req_id, layout))
                if result != (None, None):
                    last_result = result
                return last_result
        except Exception:
            return last_result

    @staticmethod
    async def req_fx(prt, req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> Tuple[
        Optional[int], Optional[bytes]]:
        """Request FX contract (ASYNC - waits for response)"""
        payload = CtsDll._build_fx_pay(req_id, sym_idx, xch_idx, cur_idx)
        send_msg(prt, payload, debug=True)

        want_req_id = ascii_field(req_id)[:-1]
        layout = get_layout(prt.server_version, "contractDetails")

        last_result = (None, None)
        try:
            while True:
                done, result = await recv_until(prt, 10.0,
                                                lambda fp: CtsFrameHandler.process_cd_frame(fp, want_req_id, layout))
                if result != (None, None):
                    last_result = result
                return last_result
        except (TimeoutError, ConnectionError):  # Fixed: specific exceptions
            return last_result

    @staticmethod
    async def req_fut(prt, req_id: int, sym_idx: int, exp_yyyymm: int, xch_idx: int, cur_idx: int) -> Tuple[
        Optional[int], Optional[bytes]]:
        """Request futures contract (ASYNC - waits for response)"""
        payload = CtsDll._build_fut_pay(req_id, sym_idx, exp_yyyymm, xch_idx, cur_idx)
        send_msg(prt, payload, debug=True)  # Fixed: removed await

        want_req_id = ascii_field(req_id)[:-1]
        layout = get_layout(prt.server_version, "contractDetails")

        last_result = (None, None)
        try:
            while True:
                done, result = await recv_until(prt, 10.0,
                                                lambda fp: CtsFrameHandler.process_cd_frame(fp, want_req_id, layout))
                if result != (None, None):
                    last_result = result
                return last_result
        except (TimeoutError, ConnectionError):  # Fixed: specific exceptions
            return last_result

    @staticmethod
    async def req_opt(prt, req_id: int, loc_sym: bytes, xch_idx: int) -> Tuple[Optional[int], Optional[bytes]]:
        """Request option contract (ASYNC - waits for response)"""
        payload = CtsDll._build_opt_pay(req_id, loc_sym, xch_idx)
        send_msg(prt, payload, debug=True)  # Fixed: removed await

        want_req_id = ascii_field(req_id)[:-1]
        layout = get_layout(prt.server_version, "contractDetails")

        last_result = (None, None)
        try:
            while True:
                done, result = await recv_until(prt, 10.0,
                                                lambda fp: CtsFrameHandler.process_cd_frame(fp, want_req_id, layout))
                if result != (None, None):
                    last_result = result
                return last_result
        except (TimeoutError, ConnectionError):  # Fixed: specific exceptions
            return last_result


__all__ = [
    "CtsDll"
]

def handle_except(e, last_result):
    print(f"[CTS] Error in req_stk: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
    return last_result