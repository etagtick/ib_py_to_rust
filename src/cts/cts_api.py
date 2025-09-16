#/cts/cts_api

from core.Tws import Tws
from cts.cts_cfg import TYPES
from cts.cts_dll import req_sec_def_opt_params, req_cts_det_async

class CtsApi:
    reqId = 0
    def __init__(self,slot):
        self.tws = Tws('127.0.0.1',4012,'cts',slot)
    def _req_id(self):
        self.reqId += 1
    async def retrieve_conid(self,req):
        self._req_id()
        return await req_cts_det_async(self.tws, self.reqId, req)

    async def _req_fop_parameters(self, root, exch, conid):
        self._req_id()
        prms={'root':root,'xch':exch,'sType':TYPES[3],'conid':conid}
        return await req_sec_def_opt_params(self.tws, self.reqId, prms)
