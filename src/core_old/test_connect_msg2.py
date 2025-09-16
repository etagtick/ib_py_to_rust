#!/usr/bin/env python3
import asyncio
import copy
import struct
import socket
from core.core_cfg import client_id

MSG = ['msgId', 'version', 'reqId', 'conId', 'symbol', 'secType', 'lastTradeDateOrContractMonth', 'strike', 'right',
       'multiplier', 'exchange', 'primaryExch', 'currency', 'localSymbol', 'tradingClass', 'includeExpired',
       'secIdType', 'secId', 'issuerId']

class SyncTws:
    def __init__(self, host="127.0.0.1", port=4002):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.server_version = None
        self.client_id = client_id("cts", 1)

    def send_frame(self, payload):
        frame = struct.pack(">I", len(payload)) + payload
        print(f">>> {frame.hex()}")
        self.sock.send(frame)

    def recv_frame(self):
        length_bytes = self.sock.recv(4)
        if len(length_bytes) < 4:
            return None
        length = struct.unpack(">I", length_bytes)[0]
        payload = self.sock.recv(length)
        print(f"<<< {(length_bytes + payload).hex()}")
        return payload

    def handshake(self):
        # Send handshake
        hello = b"API\x00" + struct.pack(">I", 9) + b"v157..178"
        self.sock.send(hello)

        # Get server version
        response = self.recv_frame()
        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        self.server_version = int(fields[0])
        print(f"Server version: {self.server_version}")

        # Send startApi
        start_payload = f"71\x002\x00{self.client_id}\x00\x00".encode('ascii')
        self.send_frame(start_payload)

        # Get managedAccounts and nextValidId
        for _ in range(2):
            response = self.recv_frame()
            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            print(f"Startup: {fields[0]} -> {fields}")

    def req_current_time(self):
        payload = b"49\x001\x00"
        self.send_frame(payload)
        response = self.recv_frame()
        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        print(f"Current time response: {fields}")
        return fields

    def req_positions(self):
        payload = b"61\x001\x00"
        self.send_frame(payload)

        while True:
            response = self.recv_frame()
            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            print(f"Position response: {fields}")
            if fields[0] == "62":  # positionsEnd
                break

    def req_account_summary(self, req_id=9001):
        payload = f"62\x001\x00{req_id}\x00All\x00$LEDGER\x00".encode('ascii')
        self.send_frame(payload)

        while True:
            response = self.recv_frame()
            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            print(f"Account summary: {fields}")
            if fields[0] == "64":  # accountSummaryEnd
                break

    def req_sec_def_opt_params(self, sym='SPX', sec_type='IND', conid='416904', req_id=3001):
        # secDefOptParams for SPX: underlying=SPX, exchange="", secType=FUT, conId=416904
        payload = f"78\x00{req_id}\x00{sym}\x00\x00{sec_type}\x00{conid}\x00".encode('ascii')
        self.send_frame(payload)
        while True:
            response = self.recv_frame()
            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            if fields[2] != 'CBOE':
                continue
            print(fields)
            for i in range(int(fields[6])):
                print(f"SecDef response: {f'{fields[4]}  {fields[7 + i]}'}")
            if fields[0] == "85":  # secDefOptParamsEnd
                break

    @staticmethod
    def get_fields():
        f = [''] * 19
        f[0] = '9';f[1] = '8';f[6] = '';f[7] = '0'
        f[12] = 'USD';f[15] = '0'
        return f

    @staticmethod
    def get_record(f):
        if f[3] in ['OPT', 'FOP']:
            return {(f[2],f[4].split(' ')[0],f[5],f[6],f[7]):f[12]}
        elif f[3] == 'FUT':
            return {(f[2],f[4].split(' ')[0],f[7]):f[12]}
        else: return {(f[2],f[7]):f[12]}


    def req_contract_details_gen2(self, req_id, args):
        f = SyncTws.get_fields()
        f[2] = str(req_id)
        for k,v in args.items():
            f[MSG.index(k)] = v

        payload = "\x00".join(f).encode('ascii') + b"\x00"
        self.send_frame(payload)

        gen = []
        while True:
            response = self.recv_frame()
            if not response:
                break

            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            if fields[0] == "10":  # contractDetails
                if len(fields) > 12:
                    rec = SyncTws.get_record(fields)
                    print(rec)
                    print(fields)
                    gen.append(rec)
            elif fields[0] == "52":  # contractDetailsEnd
                print(f"Permanent contract details end for req_id: {fields[1]}")
                break
            elif fields[0] == "4":  # error
                # Check if it's just an info message (codes 2104, 2107, 2158 are info, not errors)
                if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
                    print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
                    continue  # Don't break on info messages
                else:
                    print(f"Error in Permanent contract details: {fields}")
                    break
        return gen

    def req_contract_details_gen(self, req_id, args, location):
        f = SyncTws.get_fields()
        f[2] = str(req_id)
        for i,x in enumerate(location):
            f[x] = args[i]

        payload = "\x00".join(f).encode('ascii') + b"\x00"
        self.send_frame(payload)

        gen = []
        while True:
            response = self.recv_frame()
            if not response:
                break

            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            if fields[0] == "10":  # contractDetails
                if len(fields) > 12:
                    rec = SyncTws.get_record(fields)
                    print(rec)
                    print(fields)
                    gen.append(rec)
            elif fields[0] == "52":  # contractDetailsEnd
                print(f"Permanent contract details end for req_id: {fields[1]}")
                break
            elif fields[0] == "4":  # error
                # Check if it's just an info message (codes 2104, 2107, 2158 are info, not errors)
                if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
                    print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
                    continue  # Don't break on info messages
                else:
                    print(f"Error in Permanent contract details: {fields}")
                    break
        return gen

    def req_contract_details_perm(self, req_id=5001, symbol="MSFT", exchange="ARCA"):
        """
        Request contract details for futures
        """
        f = [''] * 19
        f[0] = '9';f[1] = '8';f[2] = str(req_id);f[4] = symbol;f[5] = 'IND';f[6] = '';f[7] = '0'
        f[10] = exchange;f[12] = 'USD';f[15] = '0'

        payload = "\x00".join(f).encode('ascii') + b"\x00"
        self.send_frame(payload)

        perm_details = []
        while True:
            response = self.recv_frame()
            if not response:
                break

            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            if fields[0] == "10":  # contractDetails
                if len(fields) > 12:
                    rec = {(f[4], f[10]): fields[12]}
                    print(rec)
                    perm_details.append(rec)
            elif fields[0] == "52":  # contractDetailsEnd
                print(f"Permanent contract details end for req_id: {fields[1]}")
                break
            elif fields[0] == "4":  # error
                # Check if it's just an info message (codes 2104, 2107, 2158 are info, not errors)
                if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
                    print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
                    continue  # Don't break on info messages
                else:
                    print(f"Error in Permanent contract details: {fields}")
                    break
        return perm_details

    def req_contract_details_opt(self, req_id=5002, underlying="SPY", expiry="20250910", strike="640", right="C", exchange="CBOE"):
        """
        Request contract details for futures
        """
        f = [''] * 19
        f[0]='9'; f[1]='8'; f[2]=str(req_id);f[4]=underlying;f[5]='OPT';f[6]=expiry
        f[7]=strike;f[8]=right;f[10]=exchange;f[12]='USD';f[15]='1'

        payload = "\x00".join(f).encode('ascii') + b"\x00"
        self.send_frame(payload)

        option_details = []
        while True:
            response = self.recv_frame()
            if not response:
                break

            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            if fields[0] == "10":  # contractDetails
                if len(fields) > 12:
                    rec ={(f[5],f[6],f[7],f[8],f[10]):fields[12]}
                    print(rec)

                    #print(f'({f[5]},{f[6]},{f[7]},{f[8]},{f[10]}):{fields[12]}')
                    option_details.append(rec)
                    #return fields[12]
            elif fields[0] == "52":  # contractDetailsEnd
                print(f"Option contract details end for req_id: {fields[1]}")
                break
            elif fields[0] == "4":  # error
                # Check if it's just an info message (codes 2104, 2107, 2158 are info, not errors)
                if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
                    #print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
                    continue  # Don't break on info messages
                else:
                    print(f"Error in Option contract details: {fields}")
                    break
        return option_details

    def req_contract_details_futures(self, req_id=5003, symbol="ES", expiry="202512", exchange="CME"):
        """
        Request contract details for futures
        """
        f = [''] * 19
        f[0]='9'; f[1]='8'; f[2]=str(req_id);f[4]=symbol;f[5]='FUT';f[6]=expiry;f[7]='0'
        f[10]=exchange;f[12]='USD';f[15]='1'

        payload = "\x00".join(f).encode('ascii') + b"\x00"
        self.send_frame(payload)

        futures_details = []
        while True:
            response = self.recv_frame()
            if not response:
                break

            fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
            if fields[0] == "10":  # contractDetails
                if len(fields) > 12:
                    rec =(f[2],{(f[5],f[6],f[10]):fields[12]})
                    print(rec)
                    futures_details.append(rec)
            elif fields[0] == "52":  # contractDetailsEnd
                print(f"Futures contract details end for req_id: {fields[1]}")
                break
            elif fields[0] == "4":  # error
                # Check if it's just an info message (codes 2104, 2107, 2158 are info, not errors)
                if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
                    print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
                    continue  # Don't break on info messages
                else:
                    print(f"Error in futures contract details: {fields}")
                    break
        return futures_details

    def close(self):
        self.sock.close()


def main():
    tws = SyncTws()
    try:
        tws.handshake()

        #print("=== Testing secDefOptParams ===")
        #tws.req_sec_def_opt_params()
        #
        prm1={'symbol':'MSFT','secType':'STK','exchange':'ARCA'}
        gen = tws.req_contract_details_gen2(5000, prm1)
        gen = tws.req_contract_details_gen( req_id=5001, args=["MSFT", "STK", "ARCA"], location=(4, 5, 10))
        print(f"Found {len(gen)} gen contracts")

        gen = tws.req_contract_details_gen( req_id=5002, args=["ES", "FUT", "CME","202512"], location=(4, 5, 10, 6))
        print(f"Found {len(gen)} gen contracts")


        gen = tws.req_contract_details_gen( req_id=5003, args=["SPX", "OPT", "CBOE","20250909","C","6500"], location=(4, 5, 10,6,8,7))
        print(f"Found {len(gen)} gen contracts")

        # print("\n=== Testing Contract Details for Stock ===")
        # #stock_details = tws.req_contract_details()
        # stock_details = tws.req_contract_details_perm(req_id=5001, symbol="SPX", exchange="CBOE")
        # print(f"Found {len(stock_details)} stock contracts")
        #
        # # print("\n=== Testing Contract Details for Option ===")
        # option_details = tws.req_contract_details_opt(req_id=5002, underlying="SPY", expiry="20250910", strike="640",right="C")
        # print(f"Found {len(option_details)} option contracts")
        # #
        # #print("\n=== Testing Contract Details for Futures ===")
        # futures_details = tws.req_contract_details_futures(req_id=5003, symbol="ES", expiry="202512", exchange="CME")
        # print(f"Found {len(futures_details)} futures contracts")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        tws.close()


if __name__ == "__main__":
    main()


'''
msgId (9)
version (8)
reqId
conId (if server version >= 37)
symbol
secType
lastTradeDateOrContractMonth (expiry)
strike
right
multiplier
exchange
primaryExch
currency
localSymbol
tradingClass
includeExpired
secIdType
secId
issuerId
'''

