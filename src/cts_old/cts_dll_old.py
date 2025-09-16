# cts_dll_old.py
from typing import Tuple, Optional
from cts.cts_cfg import root, exch, CUR
from core.core_cfg import get_layout
from core_old.core_enc_dll import ascii_field, build_chunks, frame
from core_old.core_dec_dll import find_field_offsets, parse_ascii_int, debug_payload
from core_old.core_msg_dll import recv_until, parse_error_frame, FrameHandler
from cts_old.cts_exc import CtsExc

# Pre-encoded data
_SYM_BYTES = [rt[0].encode('ascii') + b'\x00' for rt in root]
_XCH_BYTES = [x.encode('ascii') + b'\x00' for x in exch]
_CUR_BYTES = [c.encode('ascii') + b'\x00' for c in CUR]

# Static chunks - try higher version for server 178
_STATIC_CHUNKS = {
    'STK': {
        'prefix': b"9\x0010\x00",  # Try version 10 for server 178
        'mid1': b"\x00",
        'mid2': b"STK\x00\x00",
        'mid3': b"0\x00\x00\x00",
        'mid4': b"\x00",
        'mid5': b"\x00\x00",
        'suffix': b"\x000\x00\x00\x00\x00",
    },
    'IND': {
        'prefix': b"9\x0010\x00",  # Try version 10 for server 178
        'mid1': b"\x00",
        'mid2': b"IND\x00\x00",
        'mid3': b"0\x00\x00\x00",
        'mid4': b"\x00",
        'mid5': b"\x00\x00",
        'suffix': b"\x000\x00\x00\x00\x00",
    },
}

# SecDefOptParams chunks
_SEC_DEF_CHUNKS = {
    'prefix': b"78\x00",  # msgId 78
    'suffix': b"\x00",  # empty conId
}


class CtsFrameHandler(FrameHandler):
    @staticmethod
    def process_cd_frame(payload: bytes, want_req_id: bytes, layout: dict) -> tuple[
        bool, tuple[Optional[int], Optional[bytes]]]:
        tag = payload[:payload.find(b'\x00')] if b'\x00' in payload else payload

        if tag == b"4":
            req_id, code, message = parse_error_frame(payload)
            if req_id == want_req_id.decode('ascii', 'replace') or req_id == "-1":
                if code and not FrameHandler.is_info_error_code(code):
                    debug_payload(payload, "ERROR")
                    raise RuntimeError(f"IB error {code}: {message}")
            return False, (None, None)

        elif tag == b"10":
            debug_payload(payload, "CONTRACT_DETAILS")
            if not FrameHandler.check_req_id_match(payload, want_req_id, layout["reqId"]):
                return False, (None, None)

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

        elif tag == b"52":
            debug_payload(payload, "CONTRACT_DETAILS_END")
            return True, (None, None)

        return False, (None, None)

    @staticmethod
    def process_sec_def_frame(payload: bytes, want_req_id: bytes, layout: dict) -> tuple[
        bool, tuple[Optional[list], Optional[list]]]:
        tag = payload[:payload.find(b'\x00')] if b'\x00' in payload else payload

        if tag == b"4":
            req_id, code, message = parse_error_frame(payload)
            if req_id == want_req_id.decode('ascii', 'replace') or req_id == "-1":
                if code and not FrameHandler.is_info_error_code(code):
                    debug_payload(payload, "ERROR")
                    raise RuntimeError(f"IB error {code}: {message}")
            return False, (None, None)

        elif tag == b"84":
            debug_payload(payload, "SEC_DEF_OPT_PARAMS")
            fields = payload.split(b'\x00')
            if len(fields) > 1 and fields[1] == want_req_id:
                expirations = []
                strikes = []

                # Parse expirations/strikes from fields
                if len(fields) > 6:
                    try:
                        exp_count = int(fields[6].decode('ascii'))
                        for i in range(exp_count):
                            if 7 + i < len(fields):
                                expirations.append(fields[7 + i].decode('ascii'))

                        strike_start = 7 + exp_count
                        if strike_start < len(fields):
                            strike_count = int(fields[strike_start].decode('ascii'))
                            for i in range(strike_count):
                                if strike_start + 1 + i < len(fields):
                                    strikes.append(fields[strike_start + 1 + i].decode('ascii'))
                    except:
                        pass

                print(f"[CTS] SecDef: {len(expirations)} exp, {len(strikes)} strikes")
                return False, (expirations, strikes)

        elif tag == b"85":
            debug_payload(payload, "SEC_DEF_OPT_PARAMS_END")
            return True, (None, None)

        return False, (None, None)


class CtsDll:
    @staticmethod
    def _build_stk_pay(req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> bytes:
        chunks = _STATIC_CHUNKS['STK']
        rid_field = ascii_field(req_id)

        payload = build_chunks([
            chunks['prefix'], rid_field, chunks['mid1'], _SYM_BYTES[sym_idx],
            chunks['mid2'], chunks['mid3'], _XCH_BYTES[xch_idx],
            chunks['mid4'], _CUR_BYTES[cur_idx], chunks['mid5'], chunks['suffix']
        ])

        return frame(payload)

    @staticmethod
    def _build_ind_pay(req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> bytes:
        chunks = _STATIC_CHUNKS['IND']
        rid_field = ascii_field(req_id)

        payload = build_chunks([
            chunks['prefix'], rid_field, chunks['mid1'], _SYM_BYTES[sym_idx],
            chunks['mid2'], chunks['mid3'], _XCH_BYTES[xch_idx],
            chunks['mid4'], _CUR_BYTES[cur_idx], chunks['mid5'], chunks['suffix']
        ])

        return frame(payload)

    @staticmethod
    def _build_sec_def_pay(req_id: int, sym_idx: int, xch_idx: int, sec_type: str) -> bytes:
        chunks = _SEC_DEF_CHUNKS
        rid_field = ascii_field(req_id)
        sym_field = root[sym_idx][0].encode('ascii') + b'\x00'
        xch_field = exch[xch_idx].encode('ascii') + b'\x00'
        sec_field = sec_type.encode('ascii') + b'\x00'

        payload = build_chunks([
            chunks['prefix'], rid_field, sym_field, xch_field, sec_field, chunks['suffix']
        ])

        return frame(payload)

    @staticmethod
    def _send_raw(prt, msg: bytes, debug: bool = True) -> None:
        if debug and msg:
            hex_preview = CtsExc.safe_parse(
                lambda: ' '.join(f'{b:02x}' for b in msg[:32]),
                "invalid_msg"
            )
            print(f">>> {len(msg)} bytes: {hex_preview}...")
        if msg:
            prt.send(msg)

    @staticmethod
    async def req_stk(prt, req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> Tuple[
        Optional[int], Optional[bytes]]:
        msg = CtsDll._build_stk_pay(req_id, sym_idx, xch_idx, cur_idx)
        CtsDll._send_raw(prt, msg)

        want_req_id = CtsExc.safe_parse(lambda: ascii_field(req_id)[:-1], b"0")
        layout = CtsExc.safe_parse(
            lambda: get_layout(prt.server_version, "contractDetails"),
            {"reqId": 1, "conId": 12, "expiry": 4}
        )

        last_result = (None, None)
        try:
            while True:
                done, result = await recv_until(prt, 10.0,
                                                lambda fp: CtsFrameHandler.process_cd_frame(fp, want_req_id, layout))
                if result != (None, None):
                    last_result = result
                return last_result
        except Exception as e:
            print(f"[CTS] req_stk error: {type(e).__name__}: {e}")
            return last_result

    @staticmethod
    async def req_ind(prt, req_id: int, sym_idx: int, xch_idx: int, cur_idx: int) -> Tuple[
        Optional[int], Optional[bytes]]:
        msg = CtsDll._build_ind_pay(req_id, sym_idx, xch_idx, cur_idx)
        CtsDll._send_raw(prt, msg)

        want_req_id = CtsExc.safe_parse(lambda: ascii_field(req_id)[:-1], b"0")
        layout = CtsExc.safe_parse(
            lambda: get_layout(prt.server_version, "contractDetails"),
            {"reqId": 1, "conId": 12, "expiry": 4}
        )

        last_result = (None, None)
        try:
            while True:
                done, result = await recv_until(prt, 10.0,
                                                lambda fp: CtsFrameHandler.process_cd_frame(fp, want_req_id, layout))
                if result != (None, None):
                    last_result = result
                return last_result
        except Exception as e:
            print(f"[CTS] req_ind error: {type(e).__name__}: {e}")
            return last_result

    @staticmethod
    async def req_sec_def(prt, req_id: int, sym_idx: int, xch_idx: int, sec_type: str = "IND") -> Tuple[
        Optional[list], Optional[list]]:
        msg = CtsDll._build_sec_def_pay(req_id, sym_idx, xch_idx, sec_type)
        CtsDll._send_raw(prt, msg)

        want_req_id = CtsExc.safe_parse(lambda: ascii_field(req_id)[:-1], b"0")
        layout = CtsExc.safe_parse(
            lambda: get_layout(prt.server_version, "secDefOptParams"),
            {"reqId": 1, "exchange": 2, "underlyingConId": 3}
        )

        last_result = (None, None)
        try:
            while True:
                done, result = await recv_until(prt, 10.0,
                                                lambda fp: CtsFrameHandler.process_sec_def_frame(fp, want_req_id,
                                                                                                 layout))
                if result != (None, None):
                    last_result = result
                return last_result
        except Exception as e:
            print(f"[CTS] req_sec_def error: {type(e).__name__}: {e}")
            return last_result


__all__ = ["CtsDll"]