#!/usr/bin/env python3
import asyncio

from core.Tws import Tws
from core.core_util import E_EMPTY
from cts.cts_cfg import CtsChunks
from mkt.mkt_dll import cancel_mkt_data, sub_rt_bar_async, sub_mkt_data_async, stream_mkt_data, req_mkt_data, \
    req_mkt_data_async, sub_rt_bar, cancel_mkt_data_async, stream_mkt_data_async, sub_mkt_data


class MktApi:
    reqId = 0
    def __init__(self,slot):
        self.tws=Tws('127.0.0.1',4012,'mkt',slot)
    def rec_id(self):
        self.reqId += 1

    def sub_rt_bar(self, prms):
        self.rec_id()
        sub_rt_bar(self.tws, self.reqId, prms)

    async def sub_rt_bar_async(self, prms):
        self.rec_id()
        await sub_rt_bar_async(self.tws, self.reqId, prms)

    def sub_mkt_data(self, prms):
        self.rec_id()
        return sub_mkt_data(self.tws, self.reqId, prms)

    async def sub_mkt_data_async(self, prms):
        self.rec_id()
        return await sub_mkt_data_async(self.tws, self.reqId, prms)

    def stream_mkt_data(self,  duration_sec=10):
        return stream_mkt_data(self.tws, duration_sec)

    async def stream_mkt_data_async(self, duration_sec=10):
        return await stream_mkt_data_async(self.tws, duration_sec)

    def cancel_mkt_data(self,req_id):
        return cancel_mkt_data(self.tws, req_id)

    async def cancel_mkt_data_async(self, req_id):
        return await cancel_mkt_data_async(self.tws, req_id)

    def req_mkt_data(self, prms):
        self.rec_id()
        return req_mkt_data(self.tws, self.reqId, prms)

    async def req_mkt_data_async(self, prms):
        self.rec_id()
        return await req_mkt_data_async(self.tws, self.reqId, prms)

async def main():
    api = MktApi(3)
    #api.reqId=11
    api.tws.connect()
    print(api.reqId)
    prms = {'root': CtsChunks.E_RT_ES, 'sType':CtsChunks.E_SEC_FUT,'xch': CtsChunks.E_XCH_CME,'mul':CtsChunks.E_MUL_50, 'exp': '202509'}
    print(prms)
    req=api.sub_mkt_data(prms)
    print(req)
    #await api.cancel_mkt_data_async(6)
    #await api.cancel_mkt_data_async(11)
    #prms=FUT[4]
    #prms['lsym']=b'VXV5\x00'
    # prms={'root':CtsChunks.E_RT_ES,'xch':CtsChunks.XCH_CME,'lsym':CtsChunks.RT_ES+CtsChunks.MTH_U+CtsChunks.E_YR_25}
    # ff=await  api.req_mkt_data_async(prms)
    # # #ff = [x for x in ff if x is not None]
    # if ff is not None:
    #     print(ff)
    # #     print(type(ff))
    # #     print(len(ff))
    #     print(len(ff[1]))
    ff=api.stream_mkt_data(20)
    print(ff)
    print(api.cancel_mkt_data(req))
    api.tws.close()


if __name__ == "__main__":
    asyncio.run(main())