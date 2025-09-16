#!/usr/bin/env python3

import socket

from core.SyncTws import SyncTws
from datetime import datetime as dt

from core.core_util import encode_field, EMPTY_FIELD
from cts.cts_chunks import CtsBinaryChunks
from mkt.mkt_cfg import MktBinaryChunks

# Market data request field array
MKT_MSG = ['msgId', 'version', 'reqId', 'conId', 'symbol', 'secType', 'lastTradeDateOrContractMonth',
           'strike', 'right', 'multiplier', 'exchange', 'primaryExch', 'currency', 'localSymbol',
           'tradingClass', 'genericTickList', 'snapshot', 'regulatorySnapshot', 'mktDataOptions']
RTBAR_MSG = ['msgId (50)','version','reqId','conId','symbol','secType','lastTradeDateOrContractMonth','strike',
             'right','multiplier','exchange','primaryExch','currency','localSymbol','tradingClass','barSize',
             'whatToShow','useRTH','realTimeBarsOptions']


def req_rt_bar_binary(tws, req_id, prms):
    """Request market data using binary chunks for maximum efficiency"""
    # Build payload using pre-encoded binary chunks
    payload_parts = [MktBinaryChunks.REQ_RT_BAR_DATA,
                     MktBinaryChunks.VERSION_3,
                     encode_field(req_id),
                     encode_field(prms.get('conId', '')),
                     encode_field(prms.get('symbol', '')),
                     CtsBinaryChunks.get_sectype_chunk(prms.get('secType', '')),
                     encode_field(prms.get('expiry', '')),
                     encode_field(prms.get('strike', '0')),
                     encode_field(prms.get('right', '0')),
                     encode_field(prms.get('multiplier', '')),
                     CtsBinaryChunks.get_exchange_chunk(prms.get('exchange', '')),
                     EMPTY_FIELD,
                     CtsBinaryChunks.CUR_USD,
                     EMPTY_FIELD,
                     EMPTY_FIELD,
                     MktBinaryChunks.RT_BAR_SIZE,
                     MktBinaryChunks.get_what_to_show_chunk(prms.get('whatToShow', 'BID')),
                     encode_field(prms.get('useRth', '0')),
                     MktBinaryChunks.RT_BAR_OPTIONS_EMPTY]
    # Concatenate all binary chunks
    payload = b''.join(payload_parts)
    tws.send_frame(payload)

def req_mkt_data_binary(tws, req_id, prms):
    """Request market data using binary chunks for maximum efficiency"""
    # Build payload using pre-encoded binary chunks
    payload_parts = [MktBinaryChunks.MSG_REQ_MKT_DATA,
                     MktBinaryChunks.VERSION_11,
                     encode_field(req_id),
                     encode_field(prms.get('conId', '')),
                     encode_field(prms.get('symbol', '')),
                     CtsBinaryChunks.get_sectype_chunk(prms.get('secType', '')),
                     encode_field(prms.get('expiry', '')),
                     encode_field(prms.get('strike', '0')),
                     encode_field(prms.get('right', '')),
                     encode_field(prms.get('multiplier', '')),
                     CtsBinaryChunks.get_exchange_chunk(prms.get('exchange', '')),
                     EMPTY_FIELD,
                     CtsBinaryChunks.CUR_USD,
                     EMPTY_FIELD,
                     EMPTY_FIELD,
                     EMPTY_FIELD,
                     MktBinaryChunks.TRADE_TICKS,
                     MktBinaryChunks.SNAPSHOT_FALSE,
                     MktBinaryChunks.REGULATORY_FALSE,
                     MktBinaryChunks.MKT_OPTIONS_EMPTY]
    # Concatenate all binary chunks
    payload = b''.join(payload_parts)
    tws.send_frame(payload)


def mkt_rcv_data(tws, duration_sec=10):
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

def cancel_mkt_data(tws, req_id):
    """Cancel market data subscription"""
    payload = f"2\x001\x00{req_id}\x00".encode('ascii')
    tws.send_frame(payload)




def main():
    ip='127.0.0.1'
    port=4002
    tws = SyncTws(ip,port,'mkt',0)
    try:
        tws.handshake()

        prm1 = {'symbol':'ES','secType':'FUT','exchange':'CME','expiry':'202509', 'multiplier':'50', 'barSize':'10'}
        prm2 = {'symbol': 'ES', 'secType': 'FOP', 'exchange': 'CME', 'expiry': '20250910', 'multiplier': '50','strike':6500, 'right':'C'}

        req_rt_bar_binary(tws, 5500, prm1)

        req_mkt_data_binary(tws,5501,prm2)

        more_data = mkt_rcv_data(tws, duration_sec=60)

        print(f"Collected {len(more_data)} more trade records")

        cancel_mkt_data(tws, 5500)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        tws.close()


if __name__ == "__main__":
    main()