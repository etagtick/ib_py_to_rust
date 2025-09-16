#!/usr/bin/env python3
import copy
import socket
from core.SyncTws import SyncTws
from core.core_util import encode_field, ZERO_FIELD, EMPTY_FIELD
from cts.cts_chunks import CtsBinaryChunks
from hst.hst_cfg import HstBinaryChunks
from datetime import datetime as dt
# Historical data request field array (from Java source)
HIST_MSG = ['msgId', 'version', 'reqId', 'conId', 'symbol', 'secType', 'lastTradeDateOrContractMonth',
            'strike', 'right', 'multiplier', 'exchange', 'primaryExch', 'currency', 'localSymbol',
            'tradingClass', 'includeExpired', 'endDateTime', 'barSizeSetting','', 'durationString',
            'useRTH', 'formatDate', 'keepUpToDate', 'whatToShow', 'chartOptions']
#endDateTime

def get_fields():
    """Get default field array for historical data request"""
    f = [''] * 24
    f[0] = '20'  # msgId for reqHistoricalData
    f[1] = '6'  # version (no version field if server >= 124)
    f[3] = ''  # conId (empty for symbol-based lookup)
    f[7] = '0'  # strike
    f[11] = ''  # primaryExch (empty)
    f[12] = 'USD'  # currency
    f[13] = ''  # localSymbol (empty)
    f[14] = ''  # tradingClass (empty)
    f[15] = '0'  # includeExpired
    f[21] = '1'  # formatDate (1 = yyyyMMdd HH:mm:ss)
    f[22] = '0'  # keepUpToDate (0 = false)
    f[23] = ''  # chartOptions (empty)
    return f

def req_historical_data_binary(tws, req_id, prms):
    """Request historical data using binary chunks for maximum efficiency"""
    # Build payload using pre-encoded binary chunks
    payload_parts = [
        HstBinaryChunks.MSG_REQ_HIST_DATA,  # msgId: 20
        #HstBinaryChunks.VERSION_6,  # version: 6 (only if server < 124)
        encode_field(req_id),  # reqId
        encode_field(prms.get('conId', '')),  # conId
        encode_field(prms.get('symbol', '')),  # symbol
        CtsBinaryChunks.get_sectype_chunk(prms.get('secType', '')),  # secType
        encode_field(prms.get('lastTradeDateOrContractMonth', '')),  # expiry
        ZERO_FIELD,  # strike
        EMPTY_FIELD,  # right
        encode_field(prms.get('multiplier', '')),  # multiplier
        CtsBinaryChunks.get_exchange_chunk(prms.get('exchange', '')),  # exchange
        EMPTY_FIELD,  # primaryExch
        CtsBinaryChunks.CUR_USD,  # currency
        EMPTY_FIELD,  # localSymbol
        EMPTY_FIELD,  # tradingClass
        CtsBinaryChunks.INCLUDE_EXPIRED_FALSE,  # includeExpired
        encode_field(prms.get('endDateTime', '')),  # endDateTime
        HstBinaryChunks.get_bar_size_chunk(prms.get('barSizeSetting', '1 min')),  # barSizeSetting
        HstBinaryChunks.get_duration_chunk(prms.get('durationString', '1 D')),  # durationString
        encode_field(prms.get('useRTH', '1')),  # useRTH
        encode_field(prms.get('whatToShow', 'TRADES')),  # whatToShow
        HstBinaryChunks.FORMAT_DATE_STANDARD,  # formatDate
        HstBinaryChunks.KEEP_UP_TO_DATE_FALSE,  # keepUpToDate
        HstBinaryChunks.CHART_OPTIONS_EMPTY  # chartOptions
    ]

    # Concatenate all binary chunks
    payload = b''.join(payload_parts)
    tws.send_frame(payload)

def req_historical_data_gen(tws, req_id, prms):
    """Request historical data using parameter dictionary"""
    f = get_fields()
    f[2] = str(req_id)
    for k, v in prms.items():
        if k in HIST_MSG:
            f[HIST_MSG.index(k)] = str(v)

    payload = "\x00".join(f).encode('ascii') + b"\x00"
    tws.send_frame(payload)

def listen_historical_data(tws, duration_sec=30):
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

    tws.sock.settimeout(None)
    return hst

def cancel_historical_data(tws, req_id):
    """Cancel historical data request"""
    payload = f"25\x001\x00{req_id}\x00".encode('ascii')
    tws.send_frame(payload)



def main():
    tws = SyncTws()
    try:
        tws.handshake()

        # print("=== Testing Historical Data Binary Method ===")
        #
        # # Test ES futures historical data with binary chunks
        es_prms = {
            'symbol': 'ES',
            'secType': 'FUT',
            'exchange': 'CME',
            'lastTradeDateOrContractMonth': '202512',
            'endDateTime': '',  # Empty for most recent data
            'durationString': '1 D',  # 1 day
            'barSizeSetting': '1 min',  # 1 minute bars
            'whatToShow': 'TRADES',
            'useRTH': '1'  # Regular trading hours only
        }

        req_historical_data_binary(tws,3001, es_prms)

        # Listen for historical data
        hist_data = listen_historical_data(tws,duration_sec=30)
        print(f"Collected {len(hist_data)} historical bars with binary method")

        # Test SPX index historical data with binary chunks
        print("\n=== Testing SPX Index Historical Data Binary ===")
        spx_prms = {
            'symbol': 'SPX',
            'secType': 'IND',
            'exchange': 'CBOE',
            'endDateTime': '',
            'durationString': '2 D',  # 2 days
            'barSizeSetting': '1 hour',  # 1 hour bars
            'whatToShow': 'TRADES',
            'useRTH': '1'
        }

        req_historical_data_binary(tws,3002, spx_prms)

        # Listen for more historical data
        more_hist_data = listen_historical_data(tws,duration_sec=60)
        print(f"Collected {len(more_hist_data)} more historical bars with binary method")



    except Exception as e:
        print(f"Error: {e}")
    finally:
        tws.close()


if __name__ == "__main__":
    main()