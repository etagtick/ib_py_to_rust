import socket
import struct

from core.core_cfg import client_id


class SyncTws:
    def __init__(self, host, port, business, slot):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.server_version = None
        self.client_id = client_id(business, slot)

    def send_frame(self, payload):
        frame = struct.pack(">I", len(payload)) + payload
        print(f">>> {frame.hex()}")
        self.sock.send(frame)

    def recv_frame(self):
        length_bytes = self.sock.recv(4)
        if len(length_bytes) < 4:
            return None
        length = struct.unpack(">I", length_bytes)[0]
        payload = self.sock.recv(length)
        #print(f"<<< {(length_bytes + payload).hex()}")
        return payload

    def handshake(self):
        # Send handshake
        hello = b"API\x00" + struct.pack(">I", 9) + b"v157..178"
        self.sock.send(hello)

        # Get server version
        response = self.recv_frame()
        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        self.server_version = int(fields[0])
        print(f"Server version: {self.server_version}")

        # Send startApi
        start_payload = f"71\x002\x00{self.client_id}\x00\x00".encode('ascii')
        self.send_frame(start_payload)

        # Get managedAccounts and nextValidId
        for _ in range(2):
            response = self.recv_frame()
            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            print(f"Startup: {fields[0]} -> {fields}")

    def close(self):
        self.sock.close()