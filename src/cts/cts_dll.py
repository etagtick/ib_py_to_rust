#/cts/cts_dll
from typing import Any

from core.core_util import encode_field, E_EMPTY, E_ZERO, get_fields_if_match
from cts.cts_cfg import MSG_CONTRACT_DETAILS_END, MSG_CONTRACT_DETAILS, REQ_CONTRACT_DETAILS, MSG_OPT_PARAMS, \
    MSG_OPT_PARAMS_END, E_CUR_USD, VERSION_8, INCLUDE_EXPIRED_FALSE

MSG = ['msgId', 'version', 'reqId', 'conId', 'symbol', 'secType', 'lastTradeDateOrContractMonth', 'strike', 'right',
       'multiplier', 'exchange', 'primaryExch', 'currency', 'localSymbol', 'tradingClass', 'includeExpired',
       'secIdType', 'secId', 'issuerId']

def set_opt_params_request(req_id, prms):
    payload_parts = [b'78\x00',
                     encode_field(req_id),
                     prms.get('root', E_EMPTY),
                     prms.get('xch', E_EMPTY),
                     prms.get('sType', E_EMPTY),
                     prms.get('conid', E_ZERO)
                     ]
    # Concatenate all binary chunks
    payload = b''.join(payload_parts)
    return payload


async def req_sec_def_opt_params(tws, req_id, prms):
    payload = set_opt_params_request(req_id, prms)
    #print(payload)
    await tws.send_frame_async(payload)

    results = []
    while True:
        response = await tws.recv_frame_async()
        if not response:
            print("No Response!")
            break
        #print(response)
        end_of_field_0 = response.index(b'\x00')
        idx=response[:end_of_field_0]

        if idx == MSG_OPT_PARAMS:
            tmp = get_fields_if_match(response, MSG_OPT_PARAMS, (2, 4,6, 7, 8, 9, 10))
            if tmp[2]==b'1':
                res = tmp[:4]
            elif tmp[2]==b'2':
                res = tmp[:5]
            elif tmp[2] == b'3':
                res = tmp[:6]
            else:
                res = tmp[:7]
            res.pop(2)
            exps=[]
            for i in range(2,6):
                if len(res[i])==9:
                    exps.append(res[i])
            results.append( [prms.get('root', E_EMPTY)]+res[:2]+[exps])
        # else:
        #     print(response)
        if idx == MSG_OPT_PARAMS_END:
            print(f"secDefOptParamsEnd for req_id: {req_id}")
            break
        if idx == b'4':
            msg = b'No security definition has been found for the request'
            if msg in response:
                break
    #print(f'resul {results}')
    return results



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


async def req_cts_det_async(tws, req_id, payload):
    """Request contract details using binary chunks for maximum efficiency"""
    #payload = set_contract_request(req_id, prms)
    #print(payload)
    await tws.send_frame_async(payload)

    res = None
    while True:
        response = await tws.recv_frame_async()
        if not response:
            print("No Response!")
            break

        end_of_field_0 = response.index(b'\x00')
        idx=response[:end_of_field_0]
        #print(response)
        if idx == MSG_CONTRACT_DETAILS:
            res = _get_all_from_callback(response)
        if idx == MSG_CONTRACT_DETAILS_END:
            print(f"Contract details end for req_id: {req_id}")
            break
        if idx == b'4':
            msg = b'No security definition has been found for the request'
            if msg in response:
                break
    return res

def _get_conid_from_callback(data: bytes) -> bytes:
    if not data.startswith(b'10\x00'):
        return b''
    pos = 0
    for _ in range(12):  # 13 nulls to reach 12th value
        pos = data.find(b'\x00', pos) + 1
        if pos == 0:
            return b''
    end = data.find(b'\x00', pos)
    return data[pos:end] if end != -1 else data[pos:]

def _get_all_from_callback(data: bytes) -> list[Any]:
    if not data.startswith(b'10\x00'):
        return []
    res=[]
    pos = 0
    for _ in range(12):  # 13 nulls to reach 12th value
        pos = data.find(b'\x00', pos) + 1
        if pos == 0:
            res.append(b'')
        end = data.find(b'\x00', pos)
        res.append(data[pos:end] if end != -1 else data[pos:])
    tmp= [x+b'\x00' for i,x in enumerate(res) if i in [1,2,3,4,5,6,10,11,12]]
    tmp[2]=int(tmp[2][:8])
    tmp[3]=float(tmp[3].replace(b'\x00',b''))
    return tmp

