import struct

from cts.cts_cfg import HISTO_KEY_FORMAT
from cts.cts_hst_cache import HistoCache


def dump_keys(keys):
    cache = HistoCache()
    if not cache.load():
        print("No histo cache found.")
        return

    for hex_key in keys:
        try:
            key = bytes.fromhex(hex_key)
            conid = cache.get_conid(key)
            if conid:
                print(f"Key {hex_key} -> ConId {conid.decode(errors='ignore').strip()}")
            else:
                unpacked = struct.unpack(HISTO_KEY_FORMAT, key)
                print(f"Key {hex_key} not found. Parsed: {unpacked}")
        except Exception as e:
            print(f"Invalid key {hex_key}: {e}")

if __name__ == "__main__":
    # Example: pass hex-encoded 8-byte keys
    test_keys = [
        "0102030405060708"
    ]
    dump_keys(test_keys)