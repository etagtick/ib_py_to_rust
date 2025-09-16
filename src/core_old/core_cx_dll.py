# core_cx_dll.py (optimized)
import asyncio

class Cx(asyncio.Protocol):
    """
    Ultra-lean TCP connection. Pure bytes, zero callbacks, queue-only.
    """
    __slots__ = ("_transport", "_bytes_sent", "_msgs_sent", "queue")

    def __init__(self):
        self._transport = None
        self._bytes_sent = 0
        self._msgs_sent = 0
        self.queue = asyncio.Queue()

    async def connect(self, host: str, port: int):
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_connection(lambda: self, host, port)
        self._transport = transport

    def disconnect(self):
        t = self._transport
        if t is not None:
            t.close()
            self._transport = None

    def is_connected(self) -> bool:
        return self._transport is not None

    def send(self, data: bytes):
        t = self._transport
        if t is None:
            raise ConnectionError("Not connected")
        t.write(data)
        self._bytes_sent += len(data)
        self._msgs_sent += 1

    # asyncio.Protocol - minimal overhead
    def connection_lost(self, exc):
        self._transport = None
        # Push disconnect signal to queue
        self.queue.put_nowait(b"")  # Empty bytes = disconnect signal

    def data_received(self, data):
        # Zero-copy: normalize once, queue directly
        if isinstance(data, bytearray):
            data = bytes(data)
        self.queue.put_nowait(data)

    @property
    def stats(self):
        return {
            "bytes_sent": self._bytes_sent,
            "msgs_sent": self._msgs_sent,
            "connected": self.is_connected(),
        }

# class Cx(asyncio.Protocol):
#     """
#     Low-level TCP connection wrapper.
#     Emits raw bytes only. Agnostic to protocol.
#     """
#     __slots__ = ("_on_data", "_on_disc", "_transport",
#                  "_bytes_sent", "_msgs_sent", "queue")
#
#     def __init__(self, on_data, on_disc):
#         self._on_data = on_data
#         self._on_disc = on_disc
#         self._transport = None
#         self._bytes_sent = 0
#         self._msgs_sent = 0
#         self.queue = asyncio.Queue()
#
#     async def connect(self, host: str, port: int):
#         loop = asyncio.get_running_loop()
#         transport, _ = await loop.create_connection(lambda: self, host, port)
#         self._transport = transport
#
#     def disconnect(self):
#         t = self._transport
#         if t is not None:
#             t.close()
#             self._transport = None
#
#     def is_connected(self) -> bool:
#         return self._transport is not None
#
#     def send(self, data: bytes):
#         if self._transport is None:
#             raise ConnectionError("Not connected")
#         if type(data) is not bytes:
#             raise TypeError("Cx.send expects bytes")
#         self._transport.write(data)
#         self._bytes_sent += len(data)
#         self._msgs_sent += 1
#
#     # asyncio.Protocol
#     def connection_lost(self, exc):
#         msg = str(exc) if exc else ""
#         self._transport = None
#         cb = self._on_disc
#         if cb:
#             cb(msg)
#
#     # asyncio.Protocol
#     def data_received(self, data):
#         # Windows Proactor may deliver bytearray; normalize to bytes ONCE here.
#         if isinstance(data, bytearray):
#             data = bytes(data)
#         elif type(data) is not bytes:
#             raise TypeError("Cx.data_received expects bytes or bytearray")
#         # Push raw data into the async queue
#         self.queue.put_nowait(data)
#         # Still call the callback if user provided one
#         cb = self._on_data
#         if cb:
#             cb(data)
#
#     @property
#     def stats(self):
#         return {
#             "bytes_sent": self._bytes_sent,
#             "msgs_sent": self._msgs_sent,
#             "connected": self.is_connected(),
#         }