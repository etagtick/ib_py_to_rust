#!/usr/bin/env python3
import copy
import socket
from core.core_util import encode_field, E_EMPTY, E_ZERO, get_fields_if_match
from cts.cts_cfg import CtsChunks
from hst.hst_cfg import HstChunks
from datetime import datetime as dt

from mkt.mkt_cfg import MktChunks

# Historical data request field array (from Java source)
HIST_MSG = ['msgId', 'version', 'reqId', 'conId', 'symbol', 'secType', 'lastTradeDateOrContractMonth',
            'strike', 'right', 'multiplier', 'exchange', 'primaryExch', 'currency', 'localSymbol',
            'tradingClass', 'includeExpired', 'endDateTime', 'barSizeSetting','', 'durationString',
            'useRTH', 'formatDate', 'keepUpToDate', 'whatToShow', 'chartOptions']

async def req_historical_data_binary(tws, req_id, prms):
    """Request historical data using binary chunks for maximum efficiency"""
    # Build payload using pre-encoded binary chunks
    payload_parts = [
        HstChunks.REQ_HST_DATA,  # msgId: 20
        #HstBinaryChunks.VERSION_6,  # version: 6 (only if server < 124)
        encode_field(req_id),  # reqId
        encode_field(prms.get('conId', '')),  # conId
        encode_field(prms.get('symbol', '')),  # symbol
        CtsChunks.get_sectype_chunk(prms.get('secType', '')),  # secType
        encode_field(prms.get('lastTradeDateOrContractMonth', '')),  # expiry
        E_ZERO,  # strike
        E_EMPTY,  # right
        encode_field(prms.get('multiplier', '')),  # multiplier
        CtsChunks.get_exchange_chunk_end(prms.get('exchange', '')),  # exchange
        E_EMPTY,  # primaryExch
        CtsChunks.CUR_USD,  # currency
        E_EMPTY,  # localSymbol
        E_EMPTY,  # tradingClass
        CtsChunks.INCLUDE_EXPIRED_FALSE,  # includeExpired
        encode_field(prms.get('endDateTime', '')),  # endDateTime
        HstChunks.get_bar_size_chunk(prms.get('barSizeSetting', '1 min')),  # barSizeSetting
        HstChunks.get_duration_chunk(prms.get('durationString', '1 D')),  # durationString
        encode_field(prms.get('useRTH', '1')),  # useRTH
        encode_field(prms.get('whatToShow', 'TRADES')),  # whatToShow
        HstChunks.FORMAT_DATE_STANDARD,  # formatDate
        HstChunks.KEEP_UP_TO_DATE_FALSE,  # keepUpToDate
        HstChunks.CHART_OPTIONS_EMPTY  # chartOptions
    ]

    # Concatenate all binary chunks
    payload = b''.join(payload_parts)
    await tws.send_frame(payload)

async def listen_historical_data(tws, duration_sec=30):
    """Listen for historical data callbacks"""
    import time
    start_time = time.time()
    hst = {}

    print(f"Listening for historical data for {duration_sec} seconds...")

    while time.time() - start_time < duration_sec:
        tws.sock.settimeout(1.0)
        try:
            response = tws.recv_frame()
            if not response:
                continue

            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')

            if fields[0] == "17":  # historicalData
                rec_sz=8
                nb =int(fields[4])
                tmp=copy.deepcopy(fields)
                tmp=tmp[5:]
                hst={'req_id':fields[1],'strt':dt.strptime(fields[2][:-11],'%Y%m%d %H:%M:%S'),'end':dt.strptime(fields[3][:-11],'%Y%m%d %H:%M:%S'), 'bar_count':nb ,'bars':[]}
                print(hst)
                for i in range(nb):
                    rec = []
                    for j in range(rec_sz):
                        val=tmp.pop(0)
                        if j in [1,2,3,4,6]:
                            val=float(val)
                        elif j in [5,7]:
                            val=int(val)
                        elif j == 0 and len(val)==27:
                            val = dt.strptime(val[:17], '%Y%m%d %H:%M:%S').timestamp()
                        rec.append(val)
                    print(rec)

                    rec[0] = copy.deepcopy(dt.strptime(rec[0][:-11],'%Y%m%d %H:%M:%S').timestamp())
                    hst['bars'].append(rec)
                #print (hst)


            elif fields[0] == "18":  # historicalDataEnd
                print(f"Historical data end for req_id: {fields[1]}")
                break

            elif fields[0] == "4":  # error
                if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
                    print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
                else:
                    print(f"Error: {fields}")
                    break

        except socket.timeout:
            continue
        except Exception as e:
            print(f"Error receiving data: {e}")
            break

    await tws.sock.settimeout(None)
    return hst

async def cancel_historical_data(tws, req_id):
    """Cancel historical data request"""
    payload = f"25\x001\x00{req_id}\x00".encode('ascii')
    await tws.send_frame(payload)


async def req_one_hst_bar_binary(tws, req_id, prms):
    """Request historical data using binary chunks for maximum efficiency"""
    # Build payload using pre-encoded binary chunks
    payload_parts = [
        HstChunks.REQ_HST_DATA,  # msgId: 20
        #HstBinaryChunks.VERSION_6,  # version: 6 (only if server < 124)
        encode_field(req_id),  # reqId
        prms.get('conId', E_EMPTY),  # conId
        prms.get('symbol', E_EMPTY),  # symbol
        prms.get('secType',E_EMPTY ),  # secType
        encode_field(prms.get('expiry', '')),  # expiry
        E_ZERO,  # strike
        E_EMPTY,  # right
        encode_field(prms.get('multiplier', '')),  # multiplier
        prms.get('exchange', E_EMPTY),  # exchange
        E_EMPTY,  # primaryExch
        CtsChunks.CUR_USD,  # currency
        E_EMPTY,  # localSymbol
        E_EMPTY,  # tradingClass
        CtsChunks.INCLUDE_EXPIRED_FALSE,  # includeExpired
        encode_field(prms.get('endDateTime', '')),  # endDateTime
        prms.get('barSizeSetting', HstChunks.BAR_SIZE_1_DAY),  # barSizeSetting
        prms.get('durationString',HstChunks.DURATION_1_DAY),  # durationString
        prms.get('useRTH', MktChunks.USE_RTH_TRUE),  # useRTH
        prms.get('whatToShow',MktChunks.TRADES),  # whatToShow
        HstChunks.FORMAT_DATE_STANDARD,  # formatDate
        HstChunks.KEEP_UP_TO_DATE_FALSE,  # keepUpToDate
        HstChunks.CHART_OPTIONS_EMPTY  # chartOptions
    ]

    # Concatenate all binary chunks
    payload = b''.join(payload_parts)
    #print(payload)
    await tws.send_frame(payload)

    results = None
    while True:
        response = await tws.recv_frame()
        if not response:
            print("No Response!")
            break
        #print(response)
        end_of_field_0 = response.index(b'\x00')
        idx=response[:end_of_field_0]

        if idx == HstChunks.MSG_HST_DATA:
            res = get_fields_if_match(response, HstChunks.MSG_HST_DATA, (2,9))
            results=res
            #print(res)
            break
        if idx == str(req_id).encode('ascii') :
            print(f"Hst Bar end for req_id: {req_id}")
            break
        if idx == b'4':
            msg = b'No security definition has been found for the request'
            if msg in response:
                break
        else:
            continue
    return results