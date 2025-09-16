#/cts/cts_api
from cts.cts_cdn import Cdn

from cts_hst_cache import *
import asyncio
from core.Tws import Tws
from cts.cts_cfg import FUT, PERM, FOP, CtsChunks, TYPES
from cts.cts_dll import req_sec_def_opt_params, req_cts_det_async

class CtsApi:
    reqId = 0
    def __init__(self,slot):
        self.tws = Tws('127.0.0.1',4012,'cts',slot)
    def req_id(self):
        self.reqId += 1
    async def retrieve_conid(self,req):
        self.req_id()
        return await req_cts_det_async(self.tws, self.reqId, req)

    async def _req_fop_parameters(self, root, exch, conid):
        self.req_id()

        prms={'root':root,'xch':exch,'sType':TYPES[3],'conid':conid}
        return await req_sec_def_opt_params(self.tws, self.reqId, prms)


api =CtsApi(3)



#
#     # ================ OPT RETRIEVING ===============================================================
#     @staticmethod
#     def _get_cboe_lsyms():
#         # returns a dictionary of : list of localSymbols, underlying, config dict from cts_cfg
#         return Cdn.get_all_local_symbols()
#
#     async def req_cboe_opt(self, cboe_local_sym):
#         self.rec_id()
#         ib_local_sym= f'{cboe_local_sym[:-15].ljust(6)}{cboe_local_sym[-15:]}'
#         return await req_cts_det_async(self.tws, self.reqId, {'lSym':ib_local_sym, 'xch':CtsChunks.E_XCH_CBOE, 'sType':CtsChunks.E_SEC_OPT})
#
#     async def retrieve_all_opts(self):
#         dic = await CtsApi._get_cboe_lsyms()
#         results=[]
#         for k,v in dic.items():
#             ops, spot, cfg = v
#             for o in ops:
#                 res=await self.req_cboe_opt(o)
#                 results.append(res)
#                 print(res)
#         return results
#
#
#     # ================ FUT RETRIEVING ===============================================================
#     async def req_fut(self, idx):
#         ct, exps =  req_fut_exps(idx)
#
#         for e in exps:
#             self.rec_id()
#             print(f'reqid active : {self.reqId}')
#             result = await req_cts_det_async(self.tws, self.reqId, {'root':ct['root'], 'tc':ct['tc'], 'xch':ct['xch'], 'sType':CtsChunks.E_SEC_FUT, 'exp':e})
#             if result and result[-1]:
#                 yield result
#             else:
#                 print(f"[API FUT WARNING] Missing conid for {ct['root']} exp={e}")
#
#
#
#     # ================ PERM RETRIEVING ===============================================================
#     async def req_perm(self, idx):
#         self.rec_id()
#         ct=PERM[idx]
#         if not ct['active'] :
#             return None
#         print(f'reqid active : {self.reqId}')
#         return await req_cts_det_async(self.tws, self.reqId, {'conid':ct['conid'], 'xch':ct['xch']})
#
#     # ================ FOP RETRIEVING ===============================================================
#     async def req_fop_parameters(self, root, exch, conid):
#         self.rec_id()
#         prms={'root':root,'xch':exch,'sType':TYPES[3],'conid':conid}
#         return await req_sec_def_opt_params(self.tws, self.reqId, prms)
#
#     async def get_fop(self, idx):
#         self.rec_id()
#         ct=FOP[idx]
#         #print(ct)
#         if not ct['active'] :
#             return None
#         fut_idx = [i for i,x in FUT if x['root']==ct['root']][0]
#         #print(fut)
#         exps = req_fut_exps(fut_idx)
#         print(exps)
#         results=[]
#         for e in exps:
#             resp=await req_cts_det_async(self.tws, self.reqId, {'root':ct['root'], 'tc':ct['tc'], 'xch':ct['xch'], 'sType':TYPES[3], 'exp':e})
#             cn= await self.req_fop_parameters(resp[0],resp[4],resp[6])
#             results+=(cn)
#
#         res = {(z[0], z[1]): [(x[2], y) for x in results for y in x[3]] for z in results}
#         return res
#
#
# async def main():
#     api =CtsApi(3)
#     await api.tws.connect_async()
#
#     req_id=543
#     #ls=await api.retrieve_all_opts()
#
#     js=await Cdn.get_all_local_symbols()
#     ops= js[b'SPX\x00'][0]
#
#     for op in ops:
#
#     #lSym='SPXW  250912C06500000'
#         ls=await api.req_cboe_opt(op)
#         print(ls)
#     #
#     # for idx in range(6):
#     #     ff=await api.req_fut(idx)
#     #     if ff is not None:
#     #         print(ff)
#
#     #
#     # for idx in range(6):
#     #     ff=await api.req_perm(idx)
#     #     if ff is not None:
#     #         print(ff)
#
#     # for idx in range(4):
#     #     if idx!=2: continue
#     #     ff=await api.req_fop(idx)
#     #     if ff is not None:
#     #         print(ff)
#
#     # ff = await api.req_fop(3)
#     # if ff is not None:
#     #     print(ff)
#
#     await api.tws.close_async()
#
#
# if __name__ == "__main__":
#     asyncio.run(main())