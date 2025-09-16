#/cts/cts_hst_cache
import struct
from typing import List, Dict, Optional
from datetime import datetime as dt
from pathlib import Path

from cts.cts_cfg import HISTO_CACHE_FILE
from cts.cts_cache import HISTO_KEY_FORMAT


class HistoCache:
    def __init__(self):
        self.records: Dict[bytes, bytes] = {}  # key -> binary conid

    # === Persistence ===
    def load(self, filepath: Path = HISTO_CACHE_FILE) -> bool:
        if not filepath.exists():
            print(f"[CACHE] File {filepath} not found")
            return False
        try:
            with open(filepath, "rb") as f:
                while True:
                    if not HistoCache._import_record_from_file(self, f, ):
                        break
            print(f"[CACHE] Loaded {len(self.records)} records from {filepath}")
            return True
        except Exception as e:
            print(f"[CACHE] Error loading: {e}")
            return False

    def _import_record_from_file(self,f,):
        key = f.read(8)
        if len(key) < 8:
            return False
        conid_len = struct.unpack("<H", f.read(2))[0]
        conid_binary = f.read(conid_len)
        self.records[key] = conid_binary
        return True

    # === Records ===
    def add_record(self, key: bytes, conid_binary: bytes):
        action = "Overwriting" if key in self.records else "Adding"
        print(f"[CACHE] {action} {key.hex()} -> {conid_binary}")
        self.records[key] = conid_binary

    def get_conid(self, key: bytes) -> Optional[bytes]:
        return self.records.get(key)

    def save(self, filepath: Path = HISTO_CACHE_FILE):
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as f:
            for key, conid_binary in sorted(self.records.items()):
                HistoCache._write_record_to_file(f, key, conid_binary)
        print(f"[CACHE] Saved {len(self.records)} records to {filepath}")

    @staticmethod
    def _write_record_to_file(f, key, conid_binary):
        f.write(key)
        f.write(struct.pack("<H", len(conid_binary)))
        f.write(conid_binary)


    def purge_expired(self):
        today = dt.now()
        today_mmddy = int(f"{today.month:02d}{today.day:02d}{str(today.year)[3]}")
        expired = [k for k in self.records if struct.unpack(HISTO_KEY_FORMAT, k)[2] < today_mmddy and struct.unpack(HISTO_KEY_FORMAT, k)[2] != 0]
        for k in expired:
            del self.records[k]
        if expired:
            print(f"[CACHE] Purged {len(expired)} expired contracts")


    # === Missing detection (keys are inputs, never generated here) ===
    def find_missing_perm(self, perm_keys: List[bytes]) -> List[bytes]:
        return [key for key in perm_keys if key not in self.records]

    def find_missing_fut(self, fut_keys: List[bytes]) -> List[bytes]:
        return [key for key in fut_keys if key not in self.records]

    def find_missing_opt(self, opt_keys: List[bytes]) -> List[bytes]:
        return [key for key in opt_keys if key not in self.records]