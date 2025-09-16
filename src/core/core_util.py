# core_utils_dll.py (shared utilities)
import struct
from typing import Optional, List

ip='127.0.0.1'
port=4002

def req_current_time(tws):
    payload = b"49\x001\x00"
    tws.send_frame(payload)
    response = tws.recv_frame()
    fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
    print(f"Current time response: {fields}")
    return fields

def encode_field(value):
    """Encode a field value to bytes with null terminator"""
    if value is None or value == '':
        return b"\x00"
    return str(value).encode('ascii') + b"\x00"

# Common field chunks
E_EMPTY = b"\x00"
E_ZERO = b"0\x00"
E_ONE = b"1\x00"
E_TWO = b"2\x00"
E_THREE = b"3\x00"
E_FOUR = b"4\x00"
EMPTY = b""
ZERO = b"0"
ONE = b"1"
TWO = b"2"
THREE = b"3"
FOUR = b"4"

# Pre-computed constants (zero runtime cost)
IBKR_MAGIC = b"API\x00"
HANDSHAKE_VER = b"v157..178"
HANDSHAKE_FRAME = struct.pack(">I", len(HANDSHAKE_VER)) + HANDSHAKE_VER
HANDSHAKE_FULL = IBKR_MAGIC + HANDSHAKE_FRAME

START_API_PREFIX = b"71\x00" b"2\x00"
START_API_SUFFIX = b"\x00\x00"

# Startup detection patterns (pre-compiled)
TAG_MANAGED_ACCOUNTS = b"15"
TAG_NEXT_VALID_ID = b"9"
TAG_SERVER_VERSION = b"0"  # or whatever your handshake response uses


def frame(payload: bytes) -> bytes:
    """Single framing function - DRY principle"""
    return struct.pack(">I", len(payload)) + payload


def build_start_api(client_id: int) -> bytes:
    """Pre-built startApi with minimal ops"""
    cid_bytes = str(client_id).encode("ascii")
    payload = START_API_PREFIX + cid_bytes + START_API_SUFFIX
    return frame(payload)


def extract_frames(buf: bytearray, data: bytes) -> List[bytes]:
    """Zero-copy frame extraction"""
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


def split_fields(payload: bytes) -> List[bytes]:
    """Fast field splitting without decode"""
    return payload.split(b'\x00')


def extract_tag(payload: bytes) -> bytes:
    """Extract message tag (first field)"""
    end = payload.find(b'\x00')
    return payload[:end] if end >= 0 else payload


def hexdump(data: bytes) -> str:
    """Fast hex dump"""
    return " ".join(f"{b:02x}" for b in data)



def get_fields_if_match(data: bytes, first_field_value: b'10', fields_nb:(2,4,5,6,7,12)) -> Optional[List[bytes]]: #-> Optional[bytes]:
    # --- Step 1: Fast check on field 0 ---
    try:
        end_of_field_0 = data.index(b'\x00')
        if data[:end_of_field_0] != first_field_value:
            return None
    except ValueError:
        return None

    if not fields_nb:
        return []

    #print(data[:end_of_field_0])
    # Sort the unique target field indices to ensure a single forward pass
    sorted_targets = list(fields_nb)
    results_map = {}

    # State tracking: start with what we know about field 0.
    current_field_idx = 0
    end_of_last_field = end_of_field_0

    for target_idx in sorted_targets:
        # If the target is field 0, we already have its data.
        if target_idx == 0:
            results_map[0] = data[:end_of_field_0 + 1]
            continue

        # Calculate how many nulls to jump over.
        jumps_needed = target_idx - current_field_idx

        # This loop finds the end of the field *before* our target.
        pos_before_target = end_of_last_field
        for _ in range(jumps_needed - 1):
            pos_before_target = data.find(b'\x00', pos_before_target + 1)
            if pos_before_target == -1: return None  # Data too short

        # The next null marks the end of our target field.
        end_of_target = data.find(b'\x00', pos_before_target + 1)
        if end_of_target == -1: return None  # Data too short

        # We found it. Slice from after the previous null to and including the new one.
        results_map[target_idx] = data[pos_before_target + 1: end_of_target + 1]

        # Update state for the next jump.
        current_field_idx = target_idx
        end_of_last_field = end_of_target

    # --- Step 4: Assemble the final list in the originally requested order ---
    # This ensures the output order matches the input `fields_nb` list.
    try:
        return [results_map[n] for n in fields_nb]
    except KeyError:
        # This case should not be hit due to the checks above, but is safe.
        return None

def get_fields_if_match2(data: bytes, first_field_value: b'10') -> Optional[bytes]: #-> Optional[bytes]:
    # --- Step 1: Fast check on field 0 ---
    try:
        end_of_field_0 = data.index(b'\x00')
        if data[:end_of_field_0] != first_field_value:
            return None
    except ValueError:
        return None

    #print(data[:end_of_field_0])
    # Sort the unique target field indices to ensure a single forward pass
    target_idx = 12
    # State tracking: start with what we know about field 0.
    current_field_idx = 0
    end_of_last_field = end_of_field_0

    # Calculate how many nulls to jump over.
    jumps_needed = target_idx - current_field_idx

    # This loop finds the end of the field *before* our target.
    pos_before_target = end_of_last_field
    for _ in range(jumps_needed - 1):
        pos_before_target = data.find(b'\x00', pos_before_target + 1)
        if pos_before_target == -1: return None  # Data too short

    # The next null marks the end of our target field.
    end_of_target = data.find(b'\x00', pos_before_target + 1)
    if end_of_target == -1: return None  # Data too short

    # We found it. Slice from after the previous null to and including the new one.
    results = data[pos_before_target + 1: end_of_target + 1]

    # --- Step 4: Assemble the final list in the originally requested order ---
    # This ensures the output order matches the input `fields_nb` list.
    try:
        return results
    except KeyError:
        # This case should not be hit due to the checks above, but is safe.
        return None