#!/usr/bin/env python3
import asyncio

from core.AsyncTws import AsyncTws
from cts.cts_cfg import CtsChunks
from hst.hst_dll import req_historical_data_binary, listen_historical_data, cancel_historical_data, req_one_hst_bar_binary


class HstApi:
    reqId = 0
    def __init__(self, slot):
        self.tws = AsyncTws('127.0.0.1', 4002, 'hst', slot)

    def rec_id(self):
        self.reqId += 1

    async def sub_hst_bar(self, prms):
        self.rec_id()
        await req_historical_data_binary(self.tws, self.reqId, prms)

    async def stream_hst_bar(self, duration_sec=30):
        await listen_historical_data(self.tws, duration_sec=30)

    async def cancel_historical_data(self):
        self.rec_id()
        await cancel_historical_data(self.tws, self.reqId)

    async def req_one_hst_bar(self, prms):
        self.rec_id()
        return await req_one_hst_bar_binary(self.tws, self.reqId, prms)


async def main():
    api = HstApi(3)
    await api.tws.connect()

    #prms={'symbol':CtsChunks.ES,'expiry':'202509','exchange':CtsChunks.EXCH_CME,'secType':CtsChunks.SEC_FUT}
    prms={'symbol':CtsChunks.CL,'expiry':'202510','exchange':CtsChunks.EXCH_NYMEX,'secType':CtsChunks.SEC_FUT}
    ff = await api.req_one_hst_bar(prms)
    #ff = [x for x in ff if x is not None]
    print(ff is None)
    if ff is not None:
        print(f'result {ff}')
        # print(type(ff))
        # print(len(ff))
        # print(len(ff[1]))


    await api.tws.close()


if __name__ == "__main__":
    asyncio.run(main())