import os, struct, asyncio
from cts_cfg import ROOT_PATH, HISTO_CACHE, PERM, FUT, CACHE_GLOBAL
from cts_tpl_dll import CtsDll

RECORD_FMT = "<BBBBIfI"   # strict: 16 bytes
RECORD_SIZE = struct.calcsize(RECORD_FMT)


class CtsCache:
    def __init__(self, base_dir="cache"):
        self.base_dir = base_dir
        self.histo_file = os.path.join(base_dir, "histo", HISTO_CACHE)
        self.histo = {}
        os.makedirs(os.path.dirname(self.histo_file), exist_ok=True)
        self._load_histo()

    # ------------------------------
    # Binary persistence
    # ------------------------------
    def append_record(self, root, tclass, right, exch, expiry, strike, conid):
        rec = struct.pack(RECORD_FMT, root, tclass, right, exch,
                          int(expiry), float(strike), int(conid))
        with open(self.histo_file, "ab") as f:
            f.write(rec)
        key = (root, tclass, right, exch, expiry, strike)
        self.histo[key] = conid

    def _load_histo(self):
        self.histo.clear()
        if not os.path.exists(self.histo_file):
            return
        with open(self.histo_file, "rb") as f:
            while chunk := f.read(RECORD_SIZE):
                root, tclass, right, exch, expiry, strike, conid = struct.unpack(RECORD_FMT, chunk)
                key = (root, tclass, right, exch, expiry, strike)
                self.histo[key] = conid

    # ------------------------------
    # Import permanent instruments
    # ------------------------------
    def import_perm(self):
        for cfg in PERM:
            key = (cfg["root"], cfg["tclass"], 0, cfg["exch"], 0, 0.0)
            if key not in self.histo:
                self.append_record(cfg["root"], cfg["tclass"], 0, cfg["exch"],
                                   0, 0.0, cfg["conid"])
        print(f"[CACHE] Imported {len(PERM)} permanent contracts")

    # ------------------------------
    # Import futures
    # ------------------------------
    async def import_fut(self, prt, req_counter):
        """
        Retrieve and persist futures contracts (FUT from config).
        """
        for cfg in FUT:
            if not cfg["active"]:
                continue

            expiries = self._gen_fut_expiries(cfg["freq"], cfg["count"])
            for expiry in expiries:
                key = (cfg["root"], cfg["tclass"], 3, cfg["exch"], expiry, 0.0)
                if key in self.histo:
                    continue

                conid, exp = await CtsDll.req_fut(
                    prt,
                    cfg["root"],   # int index
                    expiry,        # YYYYMM
                    cfg["exch"],   # int index
                    0,             # currency index
                    req_counter=req_counter
                )
                if conid:
                    self.append_record(cfg["root"], cfg["tclass"], 3, cfg["exch"],
                                       int(exp), 0.0, conid)
                    await asyncio.sleep(CACHE_GLOBAL["request_delay_sec"])

    # ------------------------------
    # Helpers
    # ------------------------------
    def _gen_fut_expiries(self, freq, count):
        """
        Generate forward future expiries (YYYYMM) based on frequency and count.
        Checks current month validity.
        """
        from datetime import datetime
        now = datetime.now()
        year, month = now.year, now.month
        expiries = []

        if freq == 1:  # Monthly
            step = 1
        elif freq == 2:  # Quarterly
            step = 3
            # align to next quarterly month
            while month % 3 != 0:
                month += 1
                if month > 12:
                    month = 1
                    year += 1
        else:
            return []

        for _ in range(count):
            expiries.append(int(f"{year}{month:02d}"))
            month += step
            if month > 12:
                month -= 12
                year += 1
        return expiries
