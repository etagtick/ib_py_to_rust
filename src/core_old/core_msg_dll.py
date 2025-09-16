# core_msg_dll.py - Generic IB message transport layer
import asyncio
from typing import Callable, Any

from core.core_cfg import INFO_ERROR_CODES
from .core_enc_dll import frame, hex_dump
from .core_dec_dll import extract_frames, extract_tag

# Generic IB protocol constants
ERROR_TAG = b"4"


def send_msg(prt, payload: bytes, debug: bool = False) -> None:
    """Send any IB message with optional debug output"""
    wire = frame(payload)
    if debug:
        print(f">>> wire ({len(wire)} bytes): {hex_dump(wire)}")
    prt.send(wire)


async def recv_until(prt, timeout_sec: float,
                     frame_processor: Callable[[bytes], tuple[bool, Any]]) -> Any:
    """
    Generic receive loop until frame_processor returns (True, result).

    Args:
        prt: Protocol connection
        timeout_sec: Total timeout for operation
        frame_processor: Function that processes each frame and returns (done, data)
                        Should return (True, result) when complete

    Returns:
        Result from frame_processor when done=True
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_sec
    buf = bytearray()

    while True:
        time_left = deadline - loop.time()
        if time_left <= 0:
            raise TimeoutError("recv_until timeout")

        chunk = await asyncio.wait_for(prt.queue.get(), timeout=time_left)

        # Handle disconnect signal
        if not chunk:
            raise ConnectionError("Connection lost during receive")

        # Process all frames in chunk
        for frame_payload in extract_frames(buf, chunk):
            done, result = frame_processor(frame_payload)
            if done:
                return result


def is_error_frame(payload: bytes) -> bool:
    """Check if frame is an IB error message (tag "4")"""
    return extract_tag(payload) == ERROR_TAG


def parse_error_frame(payload: bytes) -> tuple[str, str, str]:
    """
    Parse IB error frame into (reqId, code, message).
    Returns empty strings if parsing fails.
    """
    fields = payload.split(b'\x00')
    if len(fields) >= 5:  # ["4", "2", reqId, code, message, ...]
        return (
            fields[2].decode('ascii', 'replace'),
            fields[3].decode('ascii', 'replace'),
            fields[4].decode('ascii', 'replace')
        )
    return "", "", ""


class FrameHandler:
    """Base class for frame processing with common patterns"""

    """Base class for frame processing with common patterns"""
    @staticmethod
    def is_info_error_code(code: str) -> bool:
        try:
            return int(code) in INFO_ERROR_CODES
        except ValueError:
            return False

    @staticmethod
    def check_req_id_match(payload: bytes, want_req_id: bytes, req_id_field_idx: int) -> bool:
        """Check if frame matches expected request ID"""
        fields = payload.split(b'\x00')
        if len(fields) > req_id_field_idx:
            return fields[req_id_field_idx] == want_req_id
        return False