#!/usr/bin/env python3
from core.SyncTws import SyncTws
from core.core_util import ip, port


def req_current_time(tws):
    payload = b"49\x001\x00"
    tws.send_frame(payload)
    response = tws.recv_frame()
    fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
    print(f"Current time response: {fields}")
    return fields

def req_positions(tws):
    payload = b"61\x001\x00"
    tws.send_frame(payload)

    while True:
        response = tws.recv_frame()
        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        print(f"Position response: {fields}")
        if fields[0] == "62":  # positionsEnd
            break

def req_account_summary(tws, req_id=9001):
    payload = f"62\x001\x00{req_id}\x00All\x00$LEDGER\x00".encode('ascii')
    tws.send_frame(payload)

    while True:
        response = tws.recv_frame()
        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        print(f"Account summary: {fields}")
        if fields[0] == "64":  # accountSummaryEnd
            break

def req_sec_def_opt_params(tws, sym='SPX',sec_type='IND',conid='416904', req_id=3001):
    # secDefOptParams for SPX: underlying=SPX, exchange="", secType=FUT, conId=416904
    payload = f"78\x00{req_id}\x00{sym}\x00\x00{sec_type}\x00{conid}\x00".encode('ascii')
    tws.send_frame(payload)
    while True:
        response = tws.recv_frame()
        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        if fields[2]!='CBOE':
            continue
        print(fields)
        for i in range(int(fields[6])):
            print(f"SecDef response: {f'{fields[4]}  {fields[7+i]}'}")
        if fields[0] == "85":  # secDefOptParamsEnd
            break




def main():
    tws = SyncTws(ip,port,'cts',9)
    try:
        tws.handshake()
        print("=== Testing secDefOptParams ===")
        req_sec_def_opt_params(tws)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        tws.close()


if __name__ == "__main__":
    main()