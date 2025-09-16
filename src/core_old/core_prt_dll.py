# core_prt_dll.py (optimized)
from __future__ import annotations
import asyncio, struct
from typing import List
from core_old.core_cx_dll import Cx
from core.core_cfg import CORE_CFG


def hexdump(data: bytes) -> str:
    if type(data) is not bytes:
        raise TypeError("hexdump expects bytes")
    return " ".join(f"{b:02x}" for b in data)


class Prt:
    MinClientVersion = CORE_CFG["MinClientVersion"]
    MaxClientVersion = CORE_CFG["MaxClientVersion"]
    __slots__ = ("host", "port", "client_id", "_cx", "_ready", "_startup_ready",
                 "server_version", "_got_accounts", "_got_nextid", "_buf")

    @property
    def queue(self) -> "asyncio.Queue[bytes]":
        cx = self._cx
        if cx is None:
            raise ConnectionError("Not connected")
        return cx.queue

    def send(self, msg: bytes):
        if type(msg) is not bytes:
            raise TypeError("Prt.send expects bytes")
        cx = self._cx
        if cx is None:
            raise ConnectionError("Not connected")
        cx.send(msg)

    def __init__(self, host="127.0.0.1", port=4002, client_id=123):
        self.host = host
        self.port = port
        self.client_id = client_id
        self._cx = Cx()  # *** FIXED: No callback args for optimized Cx
        self._ready = asyncio.Event()
        self._startup_ready = asyncio.Event()
        self.server_version = None
        self._got_accounts = False
        self._got_nextid = False
        self._buf = bytearray()  # framer buffer (bytes only)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()

    def is_connected(self) -> bool:
        return self._cx.is_connected()

    # ---------- internal static helpers (your exact code) ----------
    @staticmethod
    def _frame(payload: bytes) -> bytes:
        return struct.pack(">I", len(payload)) + payload

    @staticmethod
    def _build_handshake() -> bytes:
        ver = f"v{Prt.MinClientVersion}..{Prt.MaxClientVersion}".encode("ascii")
        return b"API\x00" + Prt._frame(ver)

    @staticmethod
    def _build_start_api(client_id: int) -> bytes:
        # ["71","2", clientId, ""]
        payload = b"71\x00" b"2\x00" + str(client_id).encode("ascii") + b"\x00\x00"
        return Prt._frame(payload)

    # ---------- tiny decoder for handshake/startup only (your exact code) ----------
    @staticmethod
    def _frames(buf: bytearray, data: bytes) -> List[bytes]:
        buf.extend(data)
        out: List[bytes] = []
        o = 0
        n = len(buf)
        while n - o >= 4:
            ln = int.from_bytes(buf[o:o + 4], "big")
            if n - o - 4 < ln: break
            out.append(bytes(buf[o + 4:o + 4 + ln]))
            o += 4 + ln
        if o:
            del buf[:o]
        return out

    @staticmethod
    def _fields(payload: bytes) -> list[str]:
        # ONLY for handshake/startup readability
        s = payload.decode("utf-8", "strict")
        if s and s[-1] == "\x00":
            s = s[:-1]
        return s.split("\x00") if s else []

    @staticmethod
    def _check_startup(fields: list[str]) -> tuple[bool, bool]:
        tag = fields[0] if fields else ""
        if tag == "15":  # managedAccounts
            return True, False
        if tag == "9":  # nextValidId
            return False, True
        return False, False

    @staticmethod
    def _check_handshake(fields: list[str]):
        if len(fields) >= 2 and fields[0].isdigit():
            return int(fields[0]), fields[1]
        return None, None

    # ---------- connect / disconnect ----------
    async def connect(self):
        # TCP
        await self._cx.connect(self.host, self.port)
        print(f"TCP connected to {self.host}:{self.port}")

        # Handshake
        hello = self._build_handshake()
        print(">>> handshake:", hexdump(hello))
        self._cx.send(hello)

        # *** PERFORMANCE FIX: Process queue directly instead of callback
        # Background task to process incoming data
        asyncio.create_task(self._process_incoming())

        # wait for server_version
        await self._ready.wait()

        # startApi
        start = self._build_start_api(self.client_id)
        print(">>> startApi:", hexdump(start))
        self._cx.send(start)
        print(f"Handshake complete → ClientId {self.client_id} requested")

        # wait for managedAccounts + nextValidId
        await self._startup_ready.wait()
        print("Startup complete → safe to send requests")

    async def disconnect(self):
        if not self._cx.is_connected():
            return
        print("Disconnecting…")
        self._cx.disconnect()
        await asyncio.sleep(0)
        print("Disconnected cleanly")

    # *** NEW: Background task to process queue (replaces callback) ***
    async def _process_incoming(self):
        """Background task to process incoming data from queue"""
        try:
            while self._cx.is_connected():
                data = await self._cx.queue.get()

                # Handle disconnect signal (empty bytes)
                if not data:
                    self._on_disc("Queue disconnect signal")
                    break

                # Process data using your existing logic
                self._on_data(data)

        except asyncio.CancelledError:
            pass  # Clean shutdown
        except Exception as e:
            print(f"Error in _process_incoming: {e}")

    # ---------- your exact callback logic (unchanged) ----------
    def _on_data(self, data: bytes):
        for payload in self._frames(self._buf, data):
            fields = self._fields(payload)
            # Step A: serverVersion
            if self.server_version is None:
                sv, ct = self._check_handshake(fields)
                if sv is not None:
                    self.server_version = sv
                    print(f"Server version: {sv}, ConnTime: {ct}")
                    self._ready.set()
            # Step B: startup
            acc, nxt = self._check_startup(fields)
            if acc:
                self._got_accounts = True
                print("[DEBUG] managedAccounts:", fields)
            if nxt:
                self._got_nextid = True
                print("[DEBUG] nextValidId:", fields)
            if self._got_accounts and self._got_nextid and not self._startup_ready.is_set():
                self._startup_ready.set()

    @staticmethod
    def _on_disc(msg: str):
        print("Disconnected by server:", msg)

# from __future__ import annotations
# import asyncio, struct
# from typing import List
# from core.core_cx_dll import Cx
# from core.core_cfg import CORE_CFG
#
# def hexdump(data: bytes) -> str:
#     if type(data) is not bytes:
#         raise TypeError("hexdump expects bytes")
#     return " ".join(f"{b:02x}" for b in data)
#
# class Prt:
#     MinClientVersion = CORE_CFG["MinClientVersion"]
#     MaxClientVersion = CORE_CFG["MaxClientVersion"]
#     __slots__ = ("host","port","client_id","_cx","_ready","_startup_ready",
#                  "server_version","_got_accounts","_got_nextid","_buf")
#
#     @property
#     def queue(self) -> "asyncio.Queue[bytes]":
#         cx = self._cx
#         if cx is None:
#             raise ConnectionError("Not connected")
#         return cx.queue
#
#     def send(self, msg: bytes):
#         if type(msg) is not bytes:
#             raise TypeError("Prt.send expects bytes")
#         cx = self._cx
#         if cx is None:
#             raise ConnectionError("Not connected")
#         cx.send(msg)
#
#     def __init__(self, host="127.0.0.1", port=4002, client_id=123):
#         self.host = host
#         self.port = port
#         self.client_id = client_id
#         self._cx = Cx()
#         self._ready = asyncio.Event()
#         self._startup_ready = asyncio.Event()
#         self.server_version = None
#         self._got_accounts = False
#         self._got_nextid = False
#         self._buf = bytearray()  # framer buffer (bytes only)
#
#     async def __aenter__(self):
#         await self.connect()
#         return self
#
#     async def __aexit__(self, exc_type, exc, tb):
#         await self.disconnect()
#
#     def is_connected(self) -> bool:
#         return self._cx.is_connected()
#
#     # ---------- internal static helpers ----------
#     @staticmethod
#     def _frame(payload: bytes) -> bytes:
#         return struct.pack(">I", len(payload)) + payload
#
#     @staticmethod
#     def _build_handshake() -> bytes:
#         ver = f"v{Prt.MinClientVersion}..{Prt.MaxClientVersion}".encode("ascii")
#         return b"API\x00" + Prt._frame(ver)
#
#     @staticmethod
#     def _build_start_api(client_id: int) -> bytes:
#         # ["71","2", clientId, ""]
#         payload = b"71\x00" b"2\x00" + str(client_id).encode("ascii") + b"\x00\x00"
#         return Prt._frame(payload)
#
#     # ---------- tiny decoder for handshake/startup only ----------
#     @staticmethod
#     def _frames(buf: bytearray, data: bytes) -> List[bytes]:
#         buf.extend(data)
#         out: List[bytes] = []
#         o = 0; n = len(buf)
#         while n - o >= 4:
#             ln = int.from_bytes(buf[o:o+4], "big")
#             if n - o - 4 < ln: break
#             out.append(bytes(buf[o+4:o+4+ln]))
#             o += 4 + ln
#         if o:
#             del buf[:o]
#         return out
#
#     @staticmethod
#     def _fields(payload: bytes) -> list[str]:
#         # ONLY for handshake/startup readability
#         s = payload.decode("utf-8", "strict")
#         if s and s[-1] == "\x00":
#             s = s[:-1]
#         return s.split("\x00") if s else []
#
#     @staticmethod
#     def _check_startup(fields: list[str]) -> tuple[bool,bool]:
#         tag = fields[0] if fields else ""
#         if tag == "15":  # managedAccounts
#             return True, False
#         if tag == "9":   # nextValidId
#             return False, True
#         return False, False
#
#     @staticmethod
#     def _check_handshake(fields: list[str]):
#         if len(fields) >= 2 and fields[0].isdigit():
#             return int(fields[0]), fields[1]
#         return None, None
#
#     # ---------- connect / disconnect ----------
#     async def connect(self):
#         # TCP
#         await self._cx.connect(self.host, self.port)
#         print(f"TCP connected to {self.host}:{self.port}")
#
#         # Handshake
#         hello = self._build_handshake()
#         print(">>> handshake:", hexdump(hello))
#         self._cx.send(hello)
#
#         # wait for server_version via _on_data
#         await self._ready.wait()
#
#         # startApi
#         start = self._build_start_api(self.client_id)
#         print(">>> startApi:", hexdump(start))
#         self._cx.send(start)
#         print(f"Handshake complete → ClientId {self.client_id} requested")
#
#         # wait for managedAccounts + nextValidId
#         await self._startup_ready.wait()
#         print("Startup complete → safe to send requests")
#
#     async def disconnect(self):
#         if not self._cx.is_connected():
#             return
#         print("Disconnecting…")
#         self._cx.disconnect()
#         await asyncio.sleep(0)
#         print("Disconnected cleanly")
#
#     # ---------- callbacks ----------
#     def _on_data(self, data: bytes):
#         for payload in self._frames(self._buf, data):
#             fields = self._fields(payload)
#             # Step A: serverVersion
#             if self.server_version is None:
#                 sv, ct = self._check_handshake(fields)
#                 if sv is not None:
#                     self.server_version = sv
#                     print(f"Server version: {sv}, ConnTime: {ct}")
#                     self._ready.set()
#             # Step B: startup
#             acc, nxt = self._check_startup(fields)
#             if acc:
#                 self._got_accounts = True
#                 print("[DEBUG] managedAccounts:", fields)
#             if nxt:
#                 self._got_nextid = True
#                 print("[DEBUG] nextValidId:", fields)
#             if self._got_accounts and self._got_nextid and not self._startup_ready.is_set():
#                 self._startup_ready.set()
#
#     @staticmethod
#     def _on_disc(msg: str):
#         print("Disconnected by server:", msg)
