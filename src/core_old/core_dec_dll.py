# core_dec_dll.py - Generic binary message decoding
from typing import List, Dict, Tuple, Set


def extract_frames(buf: bytearray, data: bytes) -> List[bytes]:
    """Extract IB wire frames: 4-byte length + payload"""
    buf.extend(data)
    frames = []
    offset = 0
    buf_len = len(buf)

    while buf_len - offset >= 4:
        frame_len = int.from_bytes(buf[offset:offset + 4], "big")
        if buf_len - offset - 4 < frame_len:
            break
        frames.append(bytes(buf[offset + 4:offset + 4 + frame_len]))
        offset += 4 + frame_len

    if offset:
        del buf[:offset]

    return frames


def extract_tag(payload: bytes) -> bytes:
    """Extract message tag (first field before NUL)"""
    nul_pos = payload.find(b'\x00')
    return payload[:nul_pos] if nul_pos >= 0 else payload


def split_fields(payload: bytes) -> List[bytes]:
    """Split payload into NUL-separated fields"""
    return payload.split(b'\x00')


def find_field_offsets(payload: bytes, wanted_indices: Set[int], max_scan: int) -> Dict[int, Tuple[int, int]]:
    """Find byte offsets for specific field indices in NUL-separated payload"""
    offsets = {}
    field_idx = 0
    start_pos = 0

    for pos, byte_val in enumerate(payload):
        if byte_val == 0:  # NUL separator
            if field_idx in wanted_indices:
                offsets[field_idx] = (start_pos, pos)
                if len(offsets) == len(wanted_indices):
                    break
            if field_idx >= max_scan:
                break
            field_idx += 1
            start_pos = pos + 1

    return offsets


def parse_ascii_int(buf: bytes, start: int, end: int) -> int:
    """Parse ASCII integer from byte range (no allocation)"""
    result = 0
    for i in range(start, end):
        digit_val = buf[i] - 48  # ASCII '0' = 48
        if 0 <= digit_val <= 9:
            result = result * 10 + digit_val
        else:
            break
    return result


def debug_payload(payload: bytes, label: str = "PAYLOAD"):
    """Debug print payload with field breakdown"""
    print(f"\n=== {label} ===")
    print(f"Hex: {' '.join(f'{b:02x}' for b in payload)}")
    print(f"Len: {len(payload)}")

    fields = payload.split(b'\x00')
    print("Fields:")
    for i, field in enumerate(fields):
        text = field.decode('ascii', 'replace')
        print(f"  [{i:2d}]: '{text}' ({len(field)} bytes)")
    print(f"=== END {label} ===\n")


__all__ = [
    "extract_frames",
    "extract_tag",
    "split_fields",
    "find_field_offsets",
    "parse_ascii_int",
    "debug_payload"
]