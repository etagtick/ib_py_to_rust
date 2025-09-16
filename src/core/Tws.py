import asyncio
import struct
import socket
import signal
import atexit
from core.core_cfg import client_id


class Tws:
    is_async=False
    def __init__(self, host="127.0.0.1", port=4012,business='cts',slot=1):
        self._closed = False

        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.server_version = None
        self.client_id = client_id(business, slot)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Register cleanup handlers
        atexit.register(self._cleanup_sync)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)


    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C and other signals"""
        print(f"\nReceived signal {signum}, cleaning up...")
        if self.is_async:
            # Create new event loop for cleanup if needed
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.close_async())
                else:
                    asyncio.run(self.close_async())
            except:
                self._cleanup_sync()
        else:
            self._cleanup_sync()
        exit(0)

    #
    # def connect(self):
    #     self.is_async = False
    #     """Establish connection to TWS"""
    #     self.sock.connect((self.host, self.port))
    #     self._handshake()
    #     print(f'Connected sync at {self.host} {self.port} {self.client_id}')

    #
    async def connect_async(self):
        self.is_async = True
        """Establish connection to TWS"""
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        await self._handshake_async()
        print(f'Connected Async at {self.host} {self.port} {self.client_id}')
    #
    async def send_frame_async(self, payload):
        if not self.is_async:
            print("The connection is Sync! Cannot use this method!")
            return None
        """Send a framed message"""
        frame = struct.pack(">I", len(payload)) + payload
        print(f">>> {frame.hex()}")
        self.writer.write(frame)
        return await self.writer.drain()
    #
    #
    async def recv_frame_async(self):
        if not self.is_async:
            print("The connection is Sync! Cannot use this method!")
            return None
        """Receive a framed message"""
        length_bytes = await self.reader.read(4)
        if len(length_bytes) < 4:
            return None
        length = struct.unpack(">I", length_bytes)[0]
        payload = await self.reader.read(length)
        #print(f"<<< {(length_bytes + payload).hex()}")
        return payload
    #
    # def send_frame(self, payload):
    #     if self.is_async:
    #         print("The connection is Async! Cannot use this method!")
    #         return None
    #     frame = struct.pack(">I", len(payload)) + payload
    #     print(f">>> {frame.hex()}")
    #     return self.sock.send(frame)
    #
    # def recv_frame(self):
    #     if self.is_async:
    #         print("The connection is Async! Cannot use this method!")
    #         return None
    #     length_bytes = self.sock.recv(4)
    #     if len(length_bytes) < 4:
    #         return None
    #     length = struct.unpack(">I", length_bytes)[0]
    #     payload = self.sock.recv(length)
    #     #print(f"<<< {(length_bytes + payload).hex()}")
    #     return payload
    #
    async def _handshake_async(self):
        """Perform TWS handshake"""
        # Send handshake
        hello = b"API\x00" + struct.pack(">I", 9) + b"v157..178"
        self.writer.write(hello)
        await self.writer.drain()

        # Get server version
        response = await self.recv_frame_async()
        #fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        #self.server_version = int(fields[0])
        #print(f"Server version: {self.server_version}")
        #print(f"Server version: {response}")

        # Send startApi
        start_payload = f"71\x002\x00{self.client_id}\x00\x00".encode('ascii')
        await self.send_frame_async(start_payload)

        # Get managedAccounts and nextValidId
        for _ in range(2):
            response = await self.recv_frame_async()
            #print(f"Startup: {response}")
            #fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            #print(f"Startup: {fields[0]} -> {fields}")
    #
    # def _handshake(self):
    #     # Send handshake
    #     hello = b"API\x00" + struct.pack(">I", 9) + b"v157..178"
    #     self.sock.send(hello)
    #
    #     # Get server version
    #     response = self.recv_frame()
    #     fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
    #     self.server_version = int(fields[0])
    #     print(f"Server version: {self.server_version}")
    #
    #     # Send startApi
    #     start_payload = f"71\x002\x00{self.client_id}\x00\x00".encode('ascii')
    #     self.send_frame(start_payload)
    #
    #     # Get managedAccounts and nextValidId
    #     for _ in range(2):
    #         response = self.recv_frame()
    #         fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
    #         print(f"Startup: {fields[0]} -> {fields}")
    #
    # def set_time_out(self, value):
    #     if self.is_async:
    #         print("The connection is Async! Cannot use this method!")
    #         return
    #     self.sock.settimeout(value)
    #
    # async def req_current_time_async(self):
    #     if not self.is_async:
    #         print("The connection is Sync! Cannot use this method!")
    #         return None
    #     """Request current time"""
    #     payload = b"49\x001\x00"
    #     await self.send_frame_async(payload)
    #     response = await self.recv_frame_async()
    #     fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
    #     print(f"Current time response: {fields}")
    #     return fields
    #
    # def req_current_time(self):
    #     if self.is_async:
    #         print("The connection is Async! Cannot use this method!")
    #         return None
    #     """Request current time"""
    #     payload = b"49\x001\x00"
    #     self.send_frame(payload)
    #     response = self.recv_frame()
    #     fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
    #     print(f"Current time response: {fields}")
    #     return fields
    #
    def _cleanup_sync(self):
        """Synchronous cleanup for emergencies"""
        if not self._closed:
            try:
                if self.sock:
                    self.sock.close()
            except:
                pass
            self._closed = True

    async def close_async(self):
        if not self.is_async or self._closed:
            return

        try:
            # Send disconnect message
            disconnect_payload = b"71\x001\x00"
            await self.send_frame_async(disconnect_payload)
            await asyncio.sleep(0.1)  # Brief pause for message delivery
        except:
            pass

        try:
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
        except:
            pass

        self._closed = True
        print("Connection closed cleanly")
    #
    # def close(self):
    #     if self.is_async:
    #         print("The connection is Async! Cannot use this method!")
    #         return
    #     self.sock.close()


