import asyncio, os
from core_old.core_prt_dll import Prt
from core_old.core_enc_dll import Enc
from core_old.core_dec_dll import Dec

HOST = os.getenv("IB_HOST", "127.0.0.1")
PORT = int(os.getenv("IB_PORT", "4002"))
CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1001"))

# IB msg IDs
REQ_CONTRACT_DETAILS = "9"
CONTRACT_DETAILS     = "10"
CONTRACT_DETAILS_END = "52"
ERROR                = "4"

def build_req(sv: int, req_id: int, *, primary_exch: str, trading_class: str) -> bytes:
    """v100+ layout (serverVersion >= 100): NO 'version' field."""
    if sv < 100:
        # legacy v8 fallback (unlikely for your serverVersion=178)
        return Enc.join([
            REQ_CONTRACT_DETAILS, "8", str(req_id),
            "", "SPX", "IND", "", "0", "", "", "CBOE", "", "USD",
            "", trading_class, "0", "", ""
        ])
    return Enc.join([
        REQ_CONTRACT_DETAILS,
        str(req_id),
        "",            # conId
        "SPX",         # symbol
        "IND",         # secType
        "",            # lastTradeDateOrContractMonth
        "0",           # strike
        "",            # right
        "",            # multiplier
        "CBOE",        # exchange
        primary_exch,  # primaryExch
        "USD",         # currency
        "",            # localSymbol
        trading_class, # tradingClass
        "0",           # includeExpired
        "",            # secIdType
        "",            # secId
    ])

VARIANTS = [
    ("A: primaryExch='', tradingClass=''",  "",  ""),
    ("B: primaryExch='CBOE', tradingClass=''",  "CBOE",  ""),
    ("C: primaryExch='CBOE', tradingClass='SPX'", "CBOE", "SPX"),
]

async def read_frames(prt: Prt, deadline_sec: float):
    dec = Dec()
    loop = asyncio.get_running_loop()
    end = loop.time() + deadline_sec
    while True:
        t = end - loop.time()
        if t <= 0: return
        try:
            chunk = await asyncio.wait_for(prt.queue.get(), timeout=t)
        except asyncio.TimeoutError:
            return
        for fields in dec.feed(chunk):
            yield fields

async def run_variant(prt: Prt, sv: int, label: str, primary: str, tclass: str, req_id: int) -> bool:
    wire = build_req(sv, req_id, primary_exch=primary, trading_class=tclass)
    prt.send(wire)
    print(f">>> [{label}] reqContractDetails (reqId={req_id})")

    got = False
    async for fields in read_frames(prt, 8.0):
        if not fields: continue
        tag = fields[0]
        if tag == ERROR:
            # Surface any server complaint
            print(f"<<< [{label}] ERROR:", fields)
        elif tag == CONTRACT_DETAILS:
            if not got:
                got = True
                print(f"<<< [{label}] contractDetails (first):", fields[:12], "...")
        elif tag == CONTRACT_DETAILS_END and len(fields) >= 2 and fields[1] == str(req_id):
            print(f"<<< [{label}] contractDetailsEnd:", fields)
            break
    if not got:
        print(f"[{label}] No contract details within timeout.")
    return got

async def main():
    prt = Prt(HOST, PORT, CLIENT_ID)
    await prt.connect()  # handshake + startApi + startup waits
    try:
        sv = prt.server_version
        if not isinstance(sv, int):
            raise RuntimeError("server_version not set")
        print(f"[INFO] serverVersion={sv}, clientId={CLIENT_ID}")

        # Try small, realistic variants IB accepts for SPX index.
        req = 1
        any_ok = False
        for label, primary, tclass in VARIANTS:
            ok = await run_variant(prt, sv, label, primary, tclass, req_id=req)
            any_ok = any_ok or ok
            req += 1  # distinct reqId per variant

        if not any_ok:
            print("[INFO] All variants returned no details. Likely field mismatch (symbol/secType/exchange) "
                  "or account lacks permission for index sec-def. Try setting primaryExch='CBOE' and tradingClass='SPX', "
                  "or query by known conId if available.")
    finally:
        await prt.disconnect()

if __name__ == "__main__":
    asyncio.run(main())

