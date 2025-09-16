import asyncio
import struct

from core.core_cfg import client_id


class AsyncTws:
    def __init__(self, host="127.0.0.1", port=4002,business='cts',slot=1):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.server_version = None
        self.client_id = client_id(business, slot)

    async def connect(self):
        """Establish connection to TWS"""
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        await self._handshake()

    async def send_frame(self, payload):
        """Send a framed message"""
        frame = struct.pack(">I", len(payload)) + payload
        print(f">>> {frame.hex()}")
        self.writer.write(frame)
        await self.writer.drain()

    async def recv_frame(self):
        """Receive a framed message"""
        length_bytes = await self.reader.read(4)
        if len(length_bytes) < 4:
            return None
        length = struct.unpack(">I", length_bytes)[0]
        payload = await self.reader.read(length)
        #print(f"<<< {(length_bytes + payload).hex()}")
        return payload

    async def _handshake(self):
        """Perform TWS handshake"""
        # Send handshake
        hello = b"API\x00" + struct.pack(">I", 9) + b"v157..178"
        self.writer.write(hello)
        await self.writer.drain()

        # Get server version
        response = await self.recv_frame()
        #fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        #self.server_version = int(fields[0])
        #print(f"Server version: {self.server_version}")
        #print(f"Server version: {response}")

        # Send startApi
        start_payload = f"71\x002\x00{self.client_id}\x00\x00".encode('ascii')
        await self.send_frame(start_payload)

        # Get managedAccounts and nextValidId
        for _ in range(2):
            response = await self.recv_frame()
            #print(f"Startup: {response}")
            #fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            #print(f"Startup: {fields[0]} -> {fields}")

    async def req_current_time(self):
        """Request current time"""
        payload = b"49\x001\x00"
        await self.send_frame(payload)
        response = await self.recv_frame()
        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        print(f"Current time response: {fields}")
        return fields

    async def close(self):
        """Close the connection"""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
