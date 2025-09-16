#!/usr/bin/env python3
# test_version_mapping.py
import asyncio
from core_old.core_prt_dll import Prt
from core.core_cfg import client_id
from core_old.core_enc_dll import frame


async def test_version_mapping():
    """Test message versions against port 4002 only"""

    msg_versions = ["8", "10", "20", "50", "100", "150", "178"]

    print("TESTING MESSAGE VERSIONS ON PORT 4002")
    print("=" * 50)

    try:
        prt = Prt(host="127.0.0.1", port=4002, client_id=client_id("cts", 0))
        await prt.connect()
        srv_ver = prt.server_version
        print(f"Server version: {srv_ver}")

        for msg_ver in msg_versions:
            print(f"\n--- Testing msg v{msg_ver} ---")
            try:
                pld = b"9\x00" + msg_ver.encode(
                    'ascii') + b"\x009999\x00\x00SPY\x00STK\x00\x000\x00\x00\x00SMART\x00\x00USD\x00\x00\x000\x00\x00\x00"
                wire = frame(pld)
                prt.send(wire)

                chunk = await asyncio.wait_for(prt.queue.get(), timeout=3.0)
                if chunk:
                    print(f"v{msg_ver}: GOT RESPONSE")
                else:
                    print(f"v{msg_ver}: DISCONNECT")
                    break

            except asyncio.TimeoutError:
                print(f"v{msg_ver}: TIMEOUT")
            except Exception as e:
                print(f"v{msg_ver}: ERROR - {e}")
                break

            if not prt.is_connected():
                break

        await prt.disconnect()

    except Exception as e:
        print(f"Connection failed: {e}")


if __name__ == "__main__":
    asyncio.run(test_version_mapping())