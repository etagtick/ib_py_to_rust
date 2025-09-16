# core_enc_dll.py - Generic binary message encoding
import struct
from typing import List

def frame(payload: bytes) -> bytes:
    """Generic IB wire framing: 4-byte big-endian length + payload"""
    return struct.pack(">I", len(payload)) + payload

def ascii_bytes(i: int) -> bytes:
    """Convert integer to ASCII bytes (no NUL terminator)"""
    return str(i).encode('ascii')

def ascii_field(i: int) -> bytes:
    """Convert integer to NUL-terminated ASCII field"""
    return str(i).encode('ascii') + b'\x00'

def join_fields(fields: List[bytes]) -> bytes:
    """Join byte fields with NUL separators + trailing NUL"""
    return b'\x00'.join(fields) + b'\x00'

def build_chunks(chunks: List[bytes]) -> bytes:
    """Concatenate pre-built binary chunks (no separators)"""
    return b''.join(chunks)

def hex_dump(data: bytes) -> str:
    """Hex representation for debugging"""
    return " ".join(f"{b:02x}" for b in data)

__all__ = [
    "frame",
    "ascii_bytes",
    "ascii_field",
    "join_fields",
    "build_chunks",
    "hex_dump"
]