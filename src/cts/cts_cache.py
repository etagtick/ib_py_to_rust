import copy
import struct
from typing import List, Dict, Optional
from datetime import datetime as dt
from datetime import timedelta as td
from pathlib import Path

from core.core_util import encode_field, E_EMPTY
from cts.cts_cdn import _fetch_all_async
from cts.cts_cfg import TCLASSES, MONTHLY, QUARTERLY, INS, E_CALL, E_PUT, E_TC_SPX

CACHE_DIR = Path(__file__).resolve().parent /"cache" / "histo"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
HISTO_CACHE_FILE = CACHE_DIR / "histo_cache.bin"
HISTO_KEY_FORMAT = "<BBHI"  # 8 bytes total

RECORDS : Dict[bytes, bytes] = {}
REQS =[]
KEYS =[]

opt = []
async def gen_dico_req_key():
    for i,x in enumerate(INS):
        _req_key_from_ins(i)

    await _all_req_key_from_opt()

def _req_key_from_ins(idx:int):
    ct = INS[idx]
    if ct['sType'] in [b'IND\x00',b'STK\x00',b'CASH\x00']:
        #req = {'root': ct['root'], 'xch': ct['xch'], 'sType': ct['sType'], 'tc': ct['tc']}
        key = _gen_key(idx, 0, 0, b'\x00', ct['tc'])
        req=decode_key(key)
        if req not in REQS: REQS.append(req)
        if key not in KEYS: KEYS.append(key)
    elif ct['sType']==b'FUT\x00':
        exps = _req_fut_exps(ct)
        for e in exps:
            #req = {'root': ct['root'], 'tc': ct['tc'], 'xch': ct['xch'], 'sType': E_SEC_FUT, 'exp': str(e)}
            key = _gen_key(idx, e, 0, b'\x00', ct['tc'])
            req = decode_key(key)
            if req not in REQS: REQS.append(req)
            if key not in KEYS: KEYS.append(key)
    elif ct['sType']==b'OPT\x00':
        opt.append(ct)

def _req_fut_exps(cfg):
    def gen_fut_exp(freq, count) -> list[int]:
        """
        Generate forward future expiries (YYYYMM) based on frequency and count.
        Checks current month validity.
        """
        now = dt.now()
        year, month = now.year, now.month
        expiries = []

        if freq == MONTHLY:  # Monthly
            step = 1
            #month +=1
        elif freq == QUARTERLY:  # Quarterly
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
            expiries.append(f'{year}{month:02d}')
            month += step
            if month > 12:
                month -= 12
                year += 1
        return expiries

    if not cfg['active'] :
        return []
    return gen_fut_exp(cfg['freq'],cfg['count'])

async def _all_req_key_from_opt():
    dat = await _fetch_all_async()
    for d in dat:
        i=d['idx']
        for o in d['ops']:
            _req_key_from_cdn(i,o)

def _req_key_from_cdn(idx:int, l_sym: str) :
    """
    Parse a CDN local symbol into fields suitable for gen_key().
    """
    cfg=INS[idx]
    root = cfg["root"]
    exchange = cfg["xch"]
    step = cfg["step"]
    tc = encode_field(l_sym[:-15])
    expiry_str = l_sym[-15:-9]  # e.g. '250919'
    expiry = dt.strptime(expiry_str, "%y%m%d")
    if tc == E_TC_SPX:  # adjust AM-settled
        expiry -= td(days=1)
    exp = dt.strftime(expiry, "%Y%m%d")
    # right = 1 if l_sym[-9] == "C" else 2
    #req_right = E_CALL if l_sym[-9] == "C" else E_PUT
    req_right=encode_field(l_sym[9])
    strike = float(l_sym[-8:]) / 1000.0

    #ib_l_sym = f'{l_sym[:-15].ljust(6)}{l_sym[-15:]}'
    #req = {'lSym':ib_l_sym, 'xch':E_XCH_CBOE, 'sType':E_SEC_OPT, 'exp':exp, 'strike':strike, 'right':req_right}
    #req = {'root': root, 'xch': E_XCH_CBOE, 'tc': tc, 'exp': exp, 'strike': strike,'right': req_right}
    key = _gen_key(idx, exp, strike, req_right, tc)
    req = decode_key(key)
    if req not in REQS: REQS.append(req)
    if key not in KEYS: KEYS.append(key)





# --- Key generation ---
def _gen_key(idx: int , expiry: str| int, strike: float, right: bytes, tc: bytes) -> bytes:
    """Generate the unique 8-byte key."""
    def _encode_mmddy(date: str | int) -> int:
        """
        Convert expiry (YYYYMMDD int or 'YYMMDD' string) to mmddy (16-bit).
        FUT can pass 'YYMM' (=> MM00Y), PERM passes 0.
        """
        if date in (0, "0", None,''):
            return 0
        s = str(date)
        if len(s) == 6:  # YYMMDD
            y, m, d = int(s[:2]), int(s[2:4]), int(s[4:6])
            return int(f"{m:02d}{d:02d}{y % 10}")
        elif len(s) == 4:  # YYMM
            y, m = int(s[:2]), int(s[2:4])
            return int(f"{m:02d}00{y % 10}")
        elif len(s) == 8:  # YYYYMMDD
            yyyy, mm, dd = int(s[:4]), int(s[4:6]), int(s[6:8])
            return int(f"{mm:02d}{dd:02d}{yyyy % 10}")
        raise ValueError(f"Unsupported expiry format: {s}")
    def _get_index_safe(array: List[bytes], value: bytes) -> int:
        try:
            return array.index(value)
        except ValueError:
            array.append(value)
            return len(array) - 1
    def _encode_strike_right(_strike: float, _right: bytes) -> int:
        return 0 if _strike == 0 else (
            int(_strike*1000) if _right==E_CALL else (
                1000000000 + int(_strike*1000) if _right==E_PUT else 2000000000+ int(_strike*1000)
            )
        )
    cfg_idx = idx                                       #1byte
    tc_idx = _get_index_safe(TCLASSES, tc)              #1byte
    expiry_mmddy = _encode_mmddy(expiry)                 #2bytes
    enc_k_right = _encode_strike_right(strike, right)   #4bytes

    return struct.pack(HISTO_KEY_FORMAT,cfg_idx, tc_idx, expiry_mmddy, enc_k_right)

def decode_key(key):
    def _decode_yyyymmdd(_date) -> (int,bytes):
        dt_str=str(_date).rjust(5,'0')
        yr='202'+dt_str[-1]
        dy = dt_str[-3:-1]
        mt = dt_str[0:2]
        return yr+mt+dy
    def _decode_strike_right(tmp:int):
        if tmp == 0:
            return 0, E_EMPTY
        elif tmp < 2000000000:
            return tmp - 1000000000, E_CALL
        elif tmp < 3000000000:
            return tmp - 2000000000, E_PUT
        else:
            return 0, E_EMPTY

    prms = struct.unpack(HISTO_KEY_FORMAT,key)
    cfg_idx = prms[0]
    strike, right =  _decode_strike_right(prms[3])
    return {'root':INS[cfg_idx]['root'], 'xch':INS[cfg_idx]['xch'],'exp':_decode_yyyymmdd(prms[2]), 'strike':strike,'right':right,'tc':TCLASSES[prms[1]] }


def load(filepath: Path = HISTO_CACHE_FILE) -> bool:
    def _import_record_from_file(_f) -> bool:
        key = _f.read(8)
        if len(key) < 8:
            return False
        conid_len = struct.unpack("<H", _f.read(2))[0]
        conid_binary = _f.read(conid_len)
        RECORDS[key] = conid_binary
        return True

    if not filepath.exists():
        print(f"[CACHE] File {filepath} not found")
        return False
    try:
        with open(filepath, "rb") as f:
            while _import_record_from_file(f):
                pass
        print(f"[CACHE] Loaded {len(RECORDS)} records from {filepath}")
        return True
    except Exception as e:
        print(f"[CACHE] Error loading: {e}")
        return False


def save(filepath: Path = HISTO_CACHE_FILE):
    def _write_record_to_file(_f, _key, _conid_binary):
        _f.write(_key)
        _f.write(struct.pack("<H", len(_conid_binary)))
        _f.write(conid_binary)

    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "wb") as f:
        for key, conid_binary in sorted(RECORDS.items()):
            _write_record_to_file(f, key, conid_binary)
    print(f"[CACHE] Saved {len(RECORDS)} records to {filepath}")

def purge_expired():
    today = dt.now()
    today_mmddy = int(f"{today.month:02d}{today.day:02d}{today.year%10}")
    expired = [k for k in RECORDS if struct.unpack(HISTO_KEY_FORMAT, k)[2] < today_mmddy and struct.unpack(HISTO_KEY_FORMAT, k)[2] != 0]
    for k in expired:
        del RECORDS[k]
    if expired:
        print(f"[CACHE] Purged {len(expired)} expired contracts")

def add_record(key: bytes, conid_binary: bytes):
    action = "Overwriting" if key in RECORDS else "Adding"
    print(f"[CACHE] {action} {key.hex()} -> {conid_binary}")
    RECORDS[key] = conid_binary

def get_conid(rec: dict) -> Optional[bytes]:
    extract= copy.deepcopy(enumerate(REQS))
    for k,v in rec.items():
        extract = [x for x in extract if k in x[1].keys() and x[1][k]==v]
    key = KEYS[extract[0][0]]
    return RECORDS.get(key)



