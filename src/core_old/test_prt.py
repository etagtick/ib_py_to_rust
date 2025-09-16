# test_prt_cx_check.py
import asyncio, os
from core_old.core_prt_dll import Prt
#from core.core_dec_dll import Dec

HOST = os.getenv("IB_HOST", "127.0.0.1")
PORT = int(os.getenv("IB_PORT", "4002"))
CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1001"))

ERROR = "4"

async def dump_frames(prt: Prt, seconds: float):
    #dec = Dec()
    end = asyncio.get_running_loop().time() + seconds
    while True:
        t = end - asyncio.get_running_loop().time()
        if t <= 0:
            return
        try:
            chunk = await asyncio.wait_for(prt.queue.get(), timeout=t)
        except asyncio.TimeoutError:
            return
        for fields in dec.feed(chunk):
            print("<<<", fields)
            if fields and fields[0] == ERROR:
                # Surface server-side reason immediately
                print("<<< ERROR:", fields)

async def main():
    prt = Prt(HOST, PORT, CLIENT_ID)
    try:
        await prt.connect()  # handshake + startApi + waits for startup
        print(f"[OK] Connected: serverVersion={prt.server_version}, clientId={CLIENT_ID}")
        # After startup, just listen briefly and print whatever the server emits.
        await dump_frames(prt, 5.0)
    finally:
        await prt.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
