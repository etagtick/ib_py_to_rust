
import asyncio
from typing import Tuple, Optional
from core.core_cfg import get_layout


def _hexdump(b: bytes) -> str:
    return " ".join(f"{x:02x}" for x in b)

# ---------- low-level helpers (private, static) ----------
def _frame(payload: bytes) -> bytes:
    return len(payload).to_bytes(4, 'big') + payload

def _ascii(i: int) -> bytes:
    return str(i).encode('ascii')

def _atoi_ascii(buf: bytes, s: int, e: int) -> int:
    n = 0
    for k in range(s, e):
        c = buf[k] - 48
        if 0 <= c <= 9:
            n = n * 10 + c
        else:
            break
    return n

def _field_offsets(payload: bytes, wanted: set[int], max_idx: int):
    res = {}
    idx = 0
    s = 0
    for i, b in enumerate(payload):
        if b == 0:
            if idx in wanted:
                res[idx] = (s, i)
                if len(res) == len(wanted):
                    return res
            if idx >= max_idx:
                return res
            idx += 1
            s = i + 1
    return res

async def _recv_until(prt, req_id: int, timeout_sec: float, i_req: int, i_conid: int, i_expiry: int
) -> Tuple[Optional[int], Optional[bytes]]:
    TAG_ERR  = b"4"
    TAG_CD   = b"10"
    TAG_CDE  = b"52"
    want_req = _ascii(req_id)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec

    buf = bytearray()

    def frames(feed: bytes):
        buf.extend(feed)
        out = []
        o = 0
        n = len(buf)
        while n - o >= 4:
            ln = int.from_bytes(buf[o:o+4], 'big')
            if n - o - 4 < ln:
                break
            out.append(bytes(buf[o+4:o+4+ln]))
            o += 4 + ln
        if o:
            del buf[:o]
        return out

    conid = None
    expiry_b = None

    layout_max = max(0, i_req, i_conid, i_expiry)
    wanted = {0, i_req, i_conid, i_expiry}

    while True:
        tleft = deadline - loop.time()
        if tleft <= 0:
            raise TimeoutError(f"contractDetails timeout for reqId={req_id}")
        chunk = await asyncio.wait_for(prt.queue.get(), timeout=tleft)
        for payload in frames(chunk):
            e0 = payload.find(b'\x00', 0)
            if e0 < 0:
                continue
            tag = payload[0:e0]
            if tag == TAG_ERR:
                #continue
                # error frame format we’ve seen in your logs:
                # ['4', '2', reqId, code, message, '']
                # We extract only for this reqId (or -1 == broadcast).
                wanted_err = {2, 3, 4}  # reqId, code, message
                offs_err = _field_offsets(payload, wanted_err, 4)
                if 2 not in offs_err or 3 not in offs_err:
                    continue  # malformed; ignore
                rs, re = offs_err[2]
                rid = payload[rs:re]
                if rid != want_req and rid != b"-1":
                    continue  # not our request
                cs, ce = offs_err[3]
                code = _atoi_ascii(payload, cs, ce)
                if code in (2104, 2107, 2158):
                    # farm status/info; ignore
                    continue
                ms, me = offs_err.get(4, (None, None))
                msg = payload[ms:me].decode("utf-8", "replace") if ms is not None else ""
                raise RuntimeError(f"IB error {code} for reqId={rid.decode('ascii', 'ignore')}: {msg}")



            elif tag == TAG_CD:
                offs = _field_offsets(payload, wanted, layout_max)
                rs, re = offs[i_req]
                if payload[rs:re] != want_req:
                    continue
                cs, ce = offs[i_conid]
                es, ee = offs[i_expiry]
                conid = _atoi_ascii(payload, cs, ce)
                expiry_b = payload[es:ee]
            elif tag == TAG_CDE:
                s1 = e0 + 1
                e1 = payload.find(b'\x00', s1)
                if e1 >= 0 and payload[s1:e1] == want_req:
                    return conid, expiry_b

# ---------- request templates (v100+, serverVersion>=100) ----------
# [9, reqId, conId, symbol, secType, ltm, strike, right, multiplier, exchange, primaryExch,
#  currency, localSymbol, tradingClass, includeExpired, secIdType, secId]

_PREFIX = b"9\x00"  # msgId

# --- add/keep these helpers at top ---
def _join_fields(fields: list[bytes]) -> bytes:
    # join with NUL and add trailing NUL (IB wire format)
    return b"\x00".join(fields) + b"\x00"

def _payload_req_cd_by_conid(req_id: int, conid: int) -> bytes:
    fields = [
        b"9",
        b"8",
        str(req_id).encode("ascii"),
        str(conid).encode("ascii"),
        b"",   # symbol
        b"",   # secType
        b"",   # ltm
        b"0",  # strike
        b"",   # right
        b"",   # multiplier
        b"",   # exchange
        b"",   # primaryExch
        b"",   # currency
        b"",   # localSymbol
        b"",   # tradingClass
        b"0",  # includeExpired
        b"",   # secIdType
        b"",   # secId
    ]
    return _join_fields(fields)

def _payload_req_cd_ind2(req_id: int, symbol: bytes, exchange: bytes, currency: bytes) -> bytes:
    # conId="", ltm="", strike="0", right="", multiplier="", primaryExch="", localSymbol="",
    # tradingClass="", includeExpired="0", secIdType="", secId=""
    parts = [
        _PREFIX, _ascii(req_id), b"\x00",
        b"", b"\x00",                 # conId
        symbol, b"\x00",
        b"IND", b"\x00",
        b"", b"\x00",                 # ltm
        b"0", b"\x00",                # strike
        b"", b"\x00",                 # right
        b"", b"\x00",                 # multiplier
        exchange, b"\x00",
        b"", b"\x00",                 # primaryExch
        currency, b"\x00",
        b"", b"\x00",                 # localSymbol
        b"", b"\x00",                 # tradingClass
        b"0", b"\x00",                # includeExpired
        b"", b"\x00",                 # secIdType
        b"", b"\x00",                 # secId
    ]
    return b"".join(parts)

def _payload_req_cd_stk2(req_id: int, symbol: bytes, exchange: bytes, currency: bytes) -> bytes:
    parts = [
        _PREFIX, _ascii(req_id), b"\x00",
        b"", b"\x00",                 # conId
        symbol, b"\x00",
        b"STK", b"\x00",
        b"", b"\x00",                 # ltm
        b"0", b"\x00",
        b"", b"\x00",
        b"", b"\x00",
        exchange, b"\x00",
        b"", b"\x00",                 # primaryExch
        currency, b"\x00",
        b"", b"\x00",
        b"", b"\x00",
        b"0", b"\x00",
        b"", b"\x00",
        b"", b"\x00",
    ]
    return b"".join(parts)

def _payload_req_cd_stk(req_id: int, symbol: bytes, exchange: bytes, currency: bytes) -> bytes:
    # Legacy v8 shape (works with serverVersion 178):
    # [9, 8, reqId, conId, symbol, STK, ltm, strike, right, multiplier,
    #  exchange, primaryExch, currency, localSymbol, tradingClass, includeExpired, secIdType, secId]
    fields = [
        b"9",
        b"8",                         # <-- REQUIRED
        str(req_id).encode("ascii"),
        b"",                          # conId
        symbol,                       # e.g. b"SPY"
        b"STK",
        b"",                          # ltm
        b"0",                         # strike
        b"",                          # right
        b"",                          # multiplier
        exchange,                     # e.g. b"SMART"
        b"",                          # primaryExch
        currency,                     # e.g. b"USD"
        b"",                          # localSymbol
        b"",                          # tradingClass
        b"0",                         # includeExpired
        b"",                          # secIdType
        b"",                          # secId
    ]
    return _join_fields(fields)

def _payload_req_cd_ind(req_id: int, symbol: bytes, exchange: bytes, currency: bytes) -> bytes:
    fields = [
        b"9",
        str(req_id).encode("ascii"),
        b"",
        symbol,
        b"IND",
        b"",
        b"0",
        b"",
        b"",
        exchange,
        b"",
        currency,
        b"",
        b"",
        b"0",
        b"",
        b"",
    ]
    return _join_fields(fields)

def _payload_req_cd_fut(req_id: int, symbol: bytes, expiry: bytes, exchange: bytes, currency: bytes) -> bytes:
    fields = [
        b"9",
        str(req_id).encode("ascii"),
        b"",
        symbol,
        b"FUT",
        expiry,       # YYYYMM or YYYYMMDD
        b"0",
        b"",
        b"",
        exchange,
        b"",
        currency,
        b"",
        b"",
        b"1",         # includeExpired=1 for futures lookups
        b"",
        b"",
    ]
    return _join_fields(fields)

# ---------- Public CTS API (all static) ----------
class CtsTpl:
    @staticmethod
    async def req_idx_first(prt, req_id: int, symbol: bytes, exchange: bytes, currency: bytes,
                            timeout_sec: float = 10.0):
        payload = _payload_req_cd_ind(req_id, symbol, exchange, currency)
        wire = _frame(payload)
        print(f">>> reqContractDetails STK wire ({len(wire)} bytes):", _hexdump(wire))
        prt.send(wire)
        #prt.send(_frame(payload))
        layout = get_layout(prt.server_version, "contractDetails")
        return await _recv_until(prt, req_id, timeout_sec, layout["reqId"], layout["conId"], layout["expiry"])

    @staticmethod
    async def req_stk_first2(prt, req_id: int, symbol: bytes, exchange: bytes, currency: bytes,
                            timeout_sec: float = 10.0):
        payload = _payload_req_cd_stk(req_id, symbol, exchange, currency)
        prt.send(_frame(payload))
        layout = get_layout(prt.server_version, "contractDetails")
        return await _recv_until(prt, req_id, timeout_sec, layout["reqId"], layout["conId"], layout["expiry"])

    @staticmethod
    async def req_stk_first(prt, req_id: int, symbol: bytes, exchange: bytes, currency: bytes,
                            timeout_sec: float = 10.0):
        payload = _payload_req_cd_stk(req_id, symbol, exchange, currency)
        wire = _frame(payload)
        # DEBUG: show exactly what we send
        print(f">>> reqContractDetails STK wire ({len(wire)} bytes):", _hexdump(wire))
        prt.send(wire)
        layout = get_layout(prt.server_version, "contractDetails")
        return await _recv_until(prt, req_id, timeout_sec, layout["reqId"], layout["conId"], layout["expiry"])

    @staticmethod
    async def req_fut_first(prt, req_id: int, symbol: bytes, expiry: bytes, exchange: bytes, currency: bytes,
                            timeout_sec: float = 10.0):
        payload = _payload_req_cd_fut(req_id, symbol, expiry, exchange, currency)
        prt.send(_frame(payload))
        layout = get_layout(prt.server_version, "contractDetails")
        return await _recv_until(prt, req_id, timeout_sec, layout["reqId"], layout["conId"], layout["expiry"])

    @staticmethod
    async def req_by_conid_first(prt, req_id: int, conid: int, timeout_sec: float = 10.0):
        payload = _payload_req_cd_by_conid(req_id, conid)
        wire = _frame(payload)
        # Show exactly what we send for verification
        print(f">>> reqContractDetails by conId wire ({len(wire)} bytes):", _hexdump(wire))
        prt.send(wire)
        layout = get_layout(prt.server_version, "contractDetails")
        return await _recv_until(prt, req_id, timeout_sec, layout["reqId"], layout["conId"], layout["expiry"])