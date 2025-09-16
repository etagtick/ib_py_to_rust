#!/usr/bin/env python3
import asyncio
from core.AsyncTws import AsyncTws
from core.core_util import encode_field
from cts.cts_chunks import CtsBinaryChunks

MSG = ['msgId', 'version', 'reqId', 'conId', 'symbol', 'secType', 'lastTradeDateOrContractMonth', 'strike', 'right',
       'multiplier', 'exchange', 'primaryExch', 'currency', 'localSymbol', 'tradingClass', 'includeExpired',
       'secIdType', 'secId', 'issuerId']


async def req_sec_def_opt_params(tws, req_id, prms):
    """Request security definition option parameters"""
    payload = f"78\x00{req_id}\x00{prms['symbol']}\x00\x00{prms['secType']}\x00{prms['conid']}\x00".encode('ascii')
    await tws.send_frame(payload)

    while True:
        response = await tws.recv_frame()
        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')
        if fields[2] != 'CBOE':
            continue
        print(fields)
        for i in range(int(fields[6])):
            print(f"SecDef response: {f'{fields[4]}  {fields[7 + i]}'}")
        if fields[0] == "85":  # secDefOptParamsEnd
            break


def get_fields():
    """Get default field array"""
    f = [''] * 19
    f[0] = '9'
    f[1] = '8'
    f[6] = ''
    f[7] = '0'
    f[12] = 'USD'
    f[15] = '0'
    return f


def get_record(f):
    """Extract record key from contract details response"""
    if f[3] in ['OPT', 'FOP']:
        return {(f[2], f[4].split(' ')[0], f[5], f[6], f[7]): f[12]}
    elif f[3] == 'FUT':
        return {(f[2], f[4].split(' ')[0], f[7]): f[12]}
    else:
        return {(f[2], f[7]): f[12]}

async def req_contract_details_binary(tws, req_id, prms):
    """Request contract details using binary chunks for maximum efficiency"""
    # Build payload using pre-encoded binary chunks
    payload_parts = [CtsBinaryChunks.MSG_CONTRACT_DETAILS,
                     CtsBinaryChunks.VERSION_8,
                     encode_field(req_id),
                     encode_field(prms.get('conid', '')),
                     encode_field(prms.get('symbol', '')),
                     CtsBinaryChunks.get_sectype_chunk(prms.get('secType', '')),
                     encode_field(prms.get('expiry', '')),
                     encode_field(prms.get('strike', '0')),
                     encode_field(prms.get('right', '')),
                     encode_field(prms.get('multiplier', '')),
                     CtsBinaryChunks.get_exchange_chunk(prms.get('exchange', '')),
                     encode_field(prms.get('primaryExch', '')),
                     CtsBinaryChunks.get_currency_chunk(prms.get('currency', 'USD')),
                     encode_field(prms.get('localSymbol', '')),
                     encode_field(prms.get('tradingClass', '')),
                     CtsBinaryChunks.INCLUDE_EXPIRED_FALSE,
                     encode_field(prms.get('secIdType', '')),
                     encode_field(prms.get('secId', '')),
                     encode_field(prms.get('issuerId', ''))]
    # Add fields based on parameters, using binary chunks where possible

    # Concatenate all binary chunks
    payload = b''.join(payload_parts)
    await tws.send_frame(payload)

    results = []
    while True:
        response = await tws.recv_frame()
        if not response:
            break

        fields = response.decode('utf-8', 'replace').rstrip('\x00').split('\x00')

        if fields[0] == "10":  # contractDetails
            if len(fields) > 12:
                rec = get_record(fields)
                print(rec)
                print(fields)
                results.append(rec)

        elif fields[0] == "52":  # contractDetailsEnd
            print(f"Contract details end for req_id: {fields[1]}")
            break

        elif fields[0] == "4":  # error
            if len(fields) > 3 and fields[3] in ['2104', '2107', '2158']:
                print(f"Info message: {fields[4] if len(fields) > 4 else 'N/A'}")
                continue
            else:
                print(f"Error in {fields[3]} contract details: {fields}")
                break

    return results


async def main():
    tws = AsyncTws()
    try:
        await tws.connect()
        await tws.handshake()

        # prm2= {'symbol':'SPX', 'secType':'IND', 'conid':'416904' }
        # print("=== Testing secDefOptParams ===")
        # await req_sec_def_opt_params(tws,3001, prm2)

        # Test with binary chunks method (most efficient)
        print("=== Testing Binary Chunks Method ===")
        prm_binary = {
            'symbol': 'MSFT',
            'secType': 'STK',
            'exchange': 'ARCA'
        }
        gen_binary = await req_contract_details_binary(tws,5000, prm_binary)
        print(f"Found {len(gen_binary)} binary contracts")

        # Test futures with binary chunks
        prm_fut_binary = {
            'symbol': 'ES',
            'secType': 'FUT',
            'exchange': 'CME',
            'expiry': '202512'
        }
        gen_fut_binary = await req_contract_details_binary(tws,5001, prm_fut_binary)
        print(f"Found {len(gen_fut_binary)} futures binary contracts")


    except Exception as e:
        print(f"Error: {e}")
    finally:
        await tws.close()


if __name__ == "__main__":
    asyncio.run(main())