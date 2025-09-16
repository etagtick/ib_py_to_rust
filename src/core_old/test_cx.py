import asyncio, signal
from core_prt_dll import Prt

stop_event = asyncio.Event()

def _handle_stop(*_):
    stop_event.set()

# Works on Windows and Linux
signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)

async def main():
    try:
        async with Prt("127.0.0.1", 4002, client_id=1023) as prt:
            print("Running... press Ctrl+C to quit")
            #await stop_event.wait()
            #print("Stop requested")

            # Do work for 10 seconds, then auto-disconnect
            await asyncio.sleep(2)
            await prt.disconnect()

    except KeyboardInterrupt:
        print("KeyboardInterrupt → shutting down cleanly")
    finally:
        await prt.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
