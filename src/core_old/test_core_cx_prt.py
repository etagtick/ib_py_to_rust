
import asyncio
import os
import sys

from core_old.core_prt_dll import Prt

HOST = os.getenv("IB_HOST", "127.0.0.1")
PORT = int(os.getenv("IB_PORT", "4002"))
CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1234"))

async def main() -> int:
    prt = Prt(HOST, PORT, CLIENT_ID)
    try:
        await prt.connect()  # includes handshake + startApi + startup waits
        if not prt.is_connected():
            print("FAIL: not connected after connect()", file=sys.stderr)
            return 2
        if not isinstance(prt.server_version, int):
            print("FAIL: server_version not set (got %r)" % (prt.server_version,), file=sys.stderr)
            return 3

        # Quick sanity: queue is available (bytes-only queue of raw chunks)
        q = prt.queue
        if q.__class__.__name__ != "Queue":
            print("FAIL: prt.queue is not an asyncio.Queue", file=sys.stderr)
            return 4

        print(f"OK: connected to {HOST}:{PORT}, serverVersion={prt.server_version}, clientId={CLIENT_ID}")
        return 0
    finally:
        await prt.disconnect()

if __name__ == "__main__":
    rc = asyncio.run(main())
    sys.exit(rc)
