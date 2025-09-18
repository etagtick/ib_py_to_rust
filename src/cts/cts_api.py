#/cts/cts_api
import asyncio
import struct
from typing import List

from core.Tws import Tws
from core.core_util import encode_field, E_EMPTY

from cts.cts_cfg import TYPES, REQ_CONTRACT_DETAILS, VERSION_8, E_CUR_USD, \
    INCLUDE_EXPIRED_FALSE, INS, E_CALL, TCLASSES, E_PUT
from cts.cts_dll import req_sec_def_opt_params, req_cts_det_async

HISTO_KEY_FORMAT = "<BBHI"  # 8 bytes total

class CtsApi:
    reqId = 0
    def __init__(self,slot):
        self.tws = Tws('127.0.0.1',4012,'cts',slot)
    def _req_id(self):
        self.reqId += 1
    async def retrieve_conid(self,req):
        self._req_id()
        return await req_cts_det_async(self.tws, self.reqId, req)

    async def _req_fop_parameters(self, root, exch, conid):
        self._req_id()
        prms={'root':root,'xch':exch,'sType':TYPES[3],'conid':conid}
        return await req_sec_def_opt_params(self.tws, self.reqId, prms)

def set_contract_request(req_id, prms):
    """Request contract details using binary chunks for maximum efficiency"""
    payload_parts = [REQ_CONTRACT_DETAILS,
                     VERSION_8,
                     encode_field(req_id),
                     prms.get('conid', E_EMPTY),
                     prms.get('root', E_EMPTY),
                     prms.get('sType', E_EMPTY),
                     encode_field(prms.get('exp', '')),
                     #prms.get('strike', E_ZERO),
                     encode_field(prms.get('strike', '')),
                     prms.get('right', E_EMPTY),
                     prms.get('mul', E_EMPTY),
                     prms.get('xch', E_EMPTY),
                     prms.get('prim', E_EMPTY),
                     prms.get('cur', E_CUR_USD),
                     encode_field(prms.get('lSym', '')),
                     prms.get('tc', E_EMPTY),
                     INCLUDE_EXPIRED_FALSE,
                     prms.get('secIdType', E_EMPTY),
                     prms.get('secId', E_EMPTY),
                     prms.get('issuerId', E_EMPTY)
                     ]
    # Concatenate all binary chunks
    payload = b''.join(payload_parts)
    return payload

def _gen_key2(callback) -> bytes:
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
    root=callback[0]
    type=callback[1]
    expiry=callback[2]
    strike=callback[3]
    right=callback[4]
    xch=callback[5]
    tc=callback[6]
    idx=[i for i,x in enumerate(INS) if x['root']==root and x['xch']==xch and x['sType']==type]
    [print(x) for x in idx]
    cfg_idx = idx[0]                                    #1byte
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
        elif tmp < 1000000000:
            return float(tmp)/1000, E_CALL
        elif tmp < 2000000000:
            return float(tmp - 2000000000)/1000, E_PUT
        else:
            return 0, E_EMPTY

    prms = struct.unpack(HISTO_KEY_FORMAT,key)
    cfg_idx = prms[0]
    strike, right =  _decode_strike_right(prms[3])
    return {'root':INS[cfg_idx]['root'], 'sType':INS[cfg_idx]['sType'], 'xch':INS[cfg_idx]['xch'],'exp':_decode_yyyymmdd(prms[2]), 'strike':strike,'right':right,'tc':TCLASSES[prms[1]] }



async def main():
    api=CtsApi(3)
    await api.tws.connect_async()
    req_id=102
    #prms={'root':E_RT_SPX,'xch':E_XCH_CBOE,'sType':E_SEC_FUT}
    #prms={'conid':INS[0]['conid'], 'xch':INS[0]['xch']}
    #prms = {'root': INS[0]['root'], 'sType':INS[0]['sType'],'xch': INS[0]['xch']}
    #idx=7
    #prms = {'root': INS[idx]['root'], 'sType': INS[idx]['sType'], 'xch': INS[idx]['xch'],'exp':'202512'}
    idx=12
    prms = {'root': INS[idx]['root'], 'sType': INS[idx]['sType'], 'xch': INS[idx]['xch'],'exp':'20250919','strike':6500,'right':E_CALL}
    print(f'prms {prms}')
    payload=set_contract_request(req_id, prms)
    #print(payload)
    resp=await req_cts_det_async(api.tws, req_id, payload)
    print(f'response : {resp}')
    key = _gen_key2(resp)
    print(f'key ')
    prms=decode_key(key)
    print(f'prms {prms}')
    payload = set_contract_request(req_id, prms)
    print(payload)
    resp = await req_cts_det_async(api.tws, req_id, payload)
    print(f'response : {resp}')
    await api.tws.close_async()

asyncio.run(main())