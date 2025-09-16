#!/usr/bin/env python3

import socket

from datetime import datetime as dt

from core.core_util import encode_field, E_EMPTY, E_ZERO
from cts.cts_cfg import CtsChunks
from mkt.mkt_cfg import MktChunks

REQ_MKT_MSG = ['MsgID','Version','reqId',
               'conId','sym','sType','exp','strike','right','mul','xch','prim','cur','lSym','tc',
               'genTk','snap','regSnap','mktOpt']



def _set_rt_bar_pld(req_id, prms):
    # Build payload using pre-encoded binary chunks
    payload_parts = [MktChunks.REQ_RT_BAR_DATA,
                     MktChunks.VERSION_3,
                     encode_field(req_id),
                     encode_field(prms.get('conId', '')),
                     encode_field(prms.get('symbol', '')),
                     CtsChunks.get_sectype_chunk_end(prms.get('secType', '')),
                     encode_field(prms.get('expiry', '')),
                     encode_field(prms.get('strike', '0')),
                     encode_field(prms.get('right', '0')),
                     encode_field(prms.get('mul', '')),
                     CtsChunks.get_exchange_chunk_end(prms.get('xch', '')),
                     E_EMPTY,
                     CtsChunks.CUR_USD,
                     E_EMPTY,
                     E_EMPTY,
                     MktChunks.RT_BAR_SIZE,
                     MktChunks.get_what_to_show_chunk(prms.get('whatToShow', 'BID')),
                     encode_field(prms.get('useRth', '0')),
                     MktChunks.RT_BAR_OPTIONS_EMPTY]
    # Concatenate all binary chunks
    return b'\x00'.join(payload_parts)


def sub_rt_bar(tws, req_id, prms):
    payload = _set_rt_bar_pld(req_id, prms)
    tws.send_frame(payload)


async def sub_rt_bar_async(tws, req_id, prms):
    payload = _set_rt_bar_pld(req_id, prms)
    await tws.send_frame(payload)


def _set_mkt_data_pld(req_id, prms, snapshot: bool):
    # This logic correctly determines the binary flags and generic ticks
    if snapshot:
        snapshot_binary = MktChunks.SNAPSHOT_TRUE
        genericTicks = E_EMPTY
    else:
        snapshot_binary = MktChunks.SNAPSHOT_FALSE
        genericTicks = MktChunks.TRADE_TICKS

    # --- THIS IS THE CORRECT, VERIFIED, SEQUENTIAL STRUCTURE ---
    payload_parts = [
        # --- Header ---
        b'1\x00',  # Field 1: MsgID (11)
        b'11\x00',  # Field 2: Version (11)
        encode_field(req_id),  # Field 3: reqId

        # --- Contract Definition (in the EXACT required order) ---
        prms.get('conId', b'\x00'),  # Field 4: conId
        prms.get('root', E_EMPTY),  # Field 5: symbol
        prms.get('sType', E_EMPTY),  # Field 6: secType
        encode_field(prms.get('exp', '')),  # Field 7: expiry
        encode_field(prms.get('strike', '0')),  # Field 8: strike
        prms.get('right', E_EMPTY),  # Field 9: right
        prms.get('mul', E_EMPTY),  # Field 10: multiplier
        prms.get('xch', E_EMPTY),  # Field 11: exchange
        prms.get('prim', b'\x00'),  # Field 12: primaryExchange
        prms.get('cur', CtsChunks.E_CUR_USD),  # Field 13: currency
        prms.get('lSym', E_EMPTY),  # Field 14: localSymbol
        prms.get('tc', E_EMPTY),  # Field 15: tradingClass

        # --- Request Parameters ---
        genericTicks,  # Field 16: genericTickList
        snapshot_binary,  # Field 17: snapshot
        E_ZERO,  # Field 18: regulatorySnapshot
        MktChunks.MKT_OPTIONS_EMPTY  # Field 19: mktDataOptions
    ]

    res = b''.join(payload_parts)
    print(res)
    return res



def sub_mkt_data(tws, req_id, prms):
    payload = _set_mkt_data_pld(req_id, prms, False)
    return tws.send_frame(payload)

async def sub_mkt_data_async(tws, req_id, prms):
    payload = _set_mkt_data_pld(req_id, prms, False)
    return await tws.send_frame_async(payload)

def cancel_mkt_data(tws, req_id):
    """Cancel market data subscription"""
    payload = f"2\x001\x00{req_id}\x00".encode('ascii')
    return tws.send_frame(payload)

async def cancel_mkt_data_async(tws, req_id):
    """Cancel market data subscription"""
    payload = f"2\x001\x00{req_id}\x00".encode('ascii')
    return await tws.send_frame_async(payload)

def stream_mkt_data(tws, duration_sec=10):
    """Listen for trade data callbacks for specified duration"""
    import time
    start_time = time.time()
    trade_records = []

    print(f"Listening for trade data for {duration_sec} seconds...")

    ins = {}
    while time.time() - start_time < duration_sec:
        tws.sock.settimeout(1.0)  # 1 second timeout
        try:
            response = tws.recv_frame()
            if not response:
                continue

            print(response)

            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            id = int(fields[3])
            if fields[3] not in ins.keys():
                ins[id]={'bar':[],'opt':[],'trd':[]}


            if fields[0] == "50":
                rec= {'ts':int(fields[3]),'op':float(fields[4]),'hi':float(fields[5]),'lo':float(fields[6]),'cl':float(fields[7])}
                ins[id]['bar'].append(rec)
                print(rec)

            if fields[0] == "21":
                if int(fields[2]) in [10,11,13]:
                    #print(fields)
                    # rec = {
                    #     'reqId': fields[1],
                    #     'tickType': fields[2],
                    #     'iv': round(float(fields[4]),6),
                    #     'dl': round(float(fields[5]),6),
                    #     'oPx': round(float(fields[6]),2),
                    #     #'dv': round(float(fields[7]),1),
                    #     'gm': round(float(fields[8]),6),
                    #     'vg': round(float(fields[9]),6),
                    #     'th': round(float(fields[10]),6),
                    #     'uPx': round(float(fields[11]),2),
                    # }
                    rec = {
                        'reqId': fields[1],
                        'tickType': fields[2],
                        'iv': int(float(fields[4])*1000000),
                        'dl': int(float(fields[5])*1000000),
                        'oPx': int(float(fields[6])*1000),
                        #'dv': int(float(fields[7]),1),
                        'gm': int(float(fields[8])*1000000),
                        'vg': int(float(fields[9])*1000000),
                        'th': int(float(fields[10])*1000000),
                        'uPx': int(float(fields[11])*1000),
                    }
                    ins[id]['opt'].append(rec)
                    print(rec)


            if fields[0] == "1":  # tickPrice
                if int(fields[3]) in [1,2,3,4]:
                    print(fields)


            elif fields[0] == "2":  # tickSize
                if fields[3] == "8":
                    print(fields)

            elif fields[0] == "46":  # ts
                if fields[3] == "45":
                    print(dt.fromtimestamp(int(fields[4])))
                elif fields[3] == "48":
                    #print(fields)
                    tmp=fields[4].split(';')
                    #print(rec)
                    last=float(tmp[0])
                    sz = int(tmp[1].split('.')[0])
                    ts=dt.fromtimestamp(float(tmp[2])/1000)
                    vol=int(tmp[3].split('.')[0])
                    vwap=float(tmp[4])
                    rec ={'ts':ts,'px':last,'sz':sz,'vol':vol,'vwap':vwap}
                    ins[id]['trd'].append(rec)


            elif fields[0] == "4":  # error
                if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
                    print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
                else:
                    print(f"Error: {fields}")

        except socket.timeout:
            continue
        except Exception as e:
            print(f"Error receiving data: {e}")
            break

    tws.sock.settimeout(None)  # Reset timeout
    return trade_records


async def stream_mkt_data_async(tws, duration_sec=10):
    """Listen for trade data callbacks for specified duration"""
    import time
    start_time = time.time()
    trade_records = []

    print(f"Listening for trade data for {duration_sec} seconds...")

    ins = {}
    while time.time() - start_time < duration_sec:
        #tws.set_time_out(1.0)  # 1 second timeout
        try:
            response = await tws.recv_frame_async()
            if not response:
                continue

            print(response)

            # fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            # id = int(fields[3])
            # if fields[3] not in ins.keys():
            #     ins[id]={'bar':[],'opt':[],'trd':[]}
            #
            #
            # if fields[0] == "50":
            #     rec= {'ts':int(fields[3]),'op':float(fields[4]),'hi':float(fields[5]),'lo':float(fields[6]),'cl':float(fields[7])}
            #     ins[id]['bar'].append(rec)
            #     print(rec)
            #
            # if fields[0] == "21":
            #     if int(fields[2]) in [10,11,13]:
            #         rec = {
            #             'reqId': fields[1],
            #             'tickType': fields[2],
            #             'iv': int(float(fields[4])*1000000),
            #             'dl': int(float(fields[5])*1000000),
            #             'oPx': int(float(fields[6])*1000),
            #             #'dv': int(float(fields[7]),1),
            #             'gm': int(float(fields[8])*1000000),
            #             'vg': int(float(fields[9])*1000000),
            #             'th': int(float(fields[10])*1000000),
            #             'uPx': int(float(fields[11])*1000),
            #         }
            #         ins[id]['opt'].append(rec)
            #         print(rec)
            #
            #
            # if fields[0] == "1":  # tickPrice
            #     if int(fields[3]) in [1,2,3,4]:
            #         print(fields)
            #
            #
            # elif fields[0] == "2":  # tickSize
            #     if fields[3] == "8":
            #         print(fields)
            #
            # elif fields[0] == "46":  # ts
            #     if fields[3] == "45":
            #         print(dt.fromtimestamp(int(fields[4])))
            #     elif fields[3] == "48":
            #         #print(fields)
            #         tmp=fields[4].split(';')
            #         #print(rec)
            #         last=float(tmp[0])
            #         sz = int(tmp[1].split('.')[0])
            #         ts=dt.fromtimestamp(float(tmp[2])/1000)
            #         vol=int(tmp[3].split('.')[0])
            #         vwap=float(tmp[4])
            #         rec ={'ts':ts,'px':last,'sz':sz,'vol':vol,'vwap':vwap}
            #         ins[id]['trd'].append(rec)
            #
            #
            # elif fields[0] == "4":  # error
            #     if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
            #         print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
            #     else:
            #         print(f"Error: {fields}")

        except socket.timeout:
            continue
        except Exception as e:
            print(f"Error receiving data: {e}")
            break

    #tws.set_time_out(None)  # Reset timeout
    return trade_records


def req_mkt_data(tws, req_id, prms):
    payload = _set_mkt_data_pld(req_id, prms, True)
    print(payload)
    tws.send_frame(payload)

    trade_records = []

    ins = {}
    while True:
        #tws.sock.settimeout(1.0)  # 1 second timeout
        try:
            response = tws.recv_frame()
            if not response:
                continue

            end_of_field_0 = response.index(b'\x00')
            idx = response[:end_of_field_0]
            # print(response)
            print(idx)

            print(response)

            # fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            # id = int(fields[3])
            # if fields[3] not in ins.keys():
            #     ins[id]={'bar':[],'opt':[],'trd':[]}
            #
            #
            # if fields[0] == "50":
            #     rec= {'ts':int(fields[3]),'op':float(fields[4]),'hi':float(fields[5]),'lo':float(fields[6]),'cl':float(fields[7])}
            #     ins[id]['bar'].append(rec)
            #     print(rec)
            #
            # if fields[0] == "21":
            #     if int(fields[2]) in [10,11,13]:
            #         rec = {
            #             'reqId': fields[1],
            #             'tickType': fields[2],
            #             'iv': int(float(fields[4])*1000000),
            #             'dl': int(float(fields[5])*1000000),
            #             'oPx': int(float(fields[6])*1000),
            #             #'dv': int(float(fields[7]),1),
            #             'gm': int(float(fields[8])*1000000),
            #             'vg': int(float(fields[9])*1000000),
            #             'th': int(float(fields[10])*1000000),
            #             'uPx': int(float(fields[11])*1000),
            #         }
            #         ins[id]['opt'].append(rec)
            #         print(rec)
            #
            #
            # if fields[0] == "1":  # tickPrice
            #     if int(fields[3]) in [1,2,3,4]:
            #         print(fields)
            #
            #
            # elif fields[0] == "2":  # tickSize
            #     if fields[3] == "8":
            #         print(fields)
            #
            # elif fields[0] == "46":  # ts
            #     if fields[3] == "45":
            #         print(dt.fromtimestamp(int(fields[4])))
            #     elif fields[3] == "48":
            #         #print(fields)
            #         tmp=fields[4].split(';')
            #         #print(rec)
            #         last=float(tmp[0])
            #         sz = int(tmp[1].split('.')[0])
            #         ts=dt.fromtimestamp(float(tmp[2])/1000)
            #         vol=int(tmp[3].split('.')[0])
            #         vwap=float(tmp[4])
            #         rec ={'ts':ts,'px':last,'sz':sz,'vol':vol,'vwap':vwap}
            #         ins[id]['trd'].append(rec)
            #
            #
            # elif fields[0] == "4":  # error
            #     if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
            #         print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
            #     else:
            #         print(f"Error: {fields}")

        except socket.timeout:
            continue
        except Exception as e:
            print(f"Error receiving data: {e}")
            break

    #tws.sock.settimeout(None)  # Reset timeout
    return trade_records

async def req_mkt_data_async(tws, req_id, prms):
    payload = _set_mkt_data_pld(req_id, prms, True)
    print(payload)
    await tws.send_frame(payload)

    trade_records = []

    ins = {}
    while True:
        try:
            response = await tws.recv_frame()
            if not response:
                #print("No Response!")
                continue

            end_of_field_0 = response.index(b'\x00')
            idx = response[:end_of_field_0]
            # print(response)
            print(idx)

            print( {response})

            if idx == b'88':
                print('exit')
                break
            # fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            # id = int(fields[3])
            # if fields[3] not in ins.keys():
            #     ins[id]={'bar':[],'opt':[],'trd':[]}
            #
            #
            # if fields[0] == "50":
            #     rec= {'ts':int(fields[3]),'op':float(fields[4]),'hi':float(fields[5]),'lo':float(fields[6]),'cl':float(fields[7])}
            #     ins[id]['bar'].append(rec)
            #     print(rec)
            #
            # if fields[0] == "21":
            #     if int(fields[2]) in [10,11,13]:
            #         rec = {
            #             'reqId': fields[1],
            #             'tickType': fields[2],
            #             'iv': int(float(fields[4])*1000000),
            #             'dl': int(float(fields[5])*1000000),
            #             'oPx': int(float(fields[6])*1000),
            #             #'dv': int(float(fields[7]),1),
            #             'gm': int(float(fields[8])*1000000),
            #             'vg': int(float(fields[9])*1000000),
            #             'th': int(float(fields[10])*1000000),
            #             'uPx': int(float(fields[11])*1000),
            #         }
            #         ins[id]['opt'].append(rec)
            #         print(rec)
            #
            #
            # if fields[0] == "1":  # tickPrice
            #     if int(fields[3]) in [1,2,3,4]:
            #         print(fields)
            #
            #
            # elif fields[0] == "2":  # tickSize
            #     if fields[3] == "8":
            #         print(fields)
            #
            # elif fields[0] == "46":  # ts
            #     if fields[3] == "45":
            #         print(dt.fromtimestamp(int(fields[4])))
            #     elif fields[3] == "48":
            #         #print(fields)
            #         tmp=fields[4].split(';')
            #         #print(rec)
            #         last=float(tmp[0])
            #         sz = int(tmp[1].split('.')[0])
            #         ts=dt.fromtimestamp(float(tmp[2])/1000)
            #         vol=int(tmp[3].split('.')[0])
            #         vwap=float(tmp[4])
            #         rec ={'ts':ts,'px':last,'sz':sz,'vol':vol,'vwap':vwap}
            #         ins[id]['trd'].append(rec)
            #
            #
            # elif fields[0] == "4":  # error
            #     if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
            #         print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
            #     else:
            #         print(f"Error: {fields}")

        except socket.timeout:
            continue
        except Exception as e:
            print(f"Error receiving data: {e}")
            break

    return trade_records