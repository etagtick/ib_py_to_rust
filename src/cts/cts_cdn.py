import copy
import math
import asyncio
import aiohttp
from datetime import datetime as dt

import requests

#import requests

from cts.cts_cfg import INS


# def _filter_options2(pload):
#     px=pload['spot']
#     ops=pload['ops']
#     idx=pload['idx']
#     step=pload['step']
#     if px is None:
#         print('parsing of cdn failed')
#         return [], float('nan')
#     lbase = math.floor(px / step) * step
#     hbase = math.ceil(px / step) * step
#     low = int((lbase - 127 * step) * 1000)
#     high = int((hbase + 127 * step) * 1000)
#     if ops is None:
#         print('parsing of cdn failed')
#         return None
#     filtered = [x for x in ops
#                 if (dt.strptime(x[-15:-9], '%y%m%d') - dt.now()).days >= 0
#                 and (
#                         (low <= int(x[-8:]) < high and len(x[:-15]) > 3)  # SPXW
#                         or (len(x[:-15]) == 3 and (dt.strptime(x[-15:-9], '%y%m%d') - dt.now()).days <= 90)
#                 )]
#     return filtered, float(px) if px is not None else float("nan")
#
# async def get_filtered_dico_async():
#     """
#     Fetch and parse all active option chains.
#     Returns dict: { ROOT → ( [localsymbols], spot_price ) }
#     """
#     raw = await _fetch_all_async()
#     results = {}
#     for x in raw:
#         symbols, spot = _filter_options(x)
#         results[x['idx']] = (symbols, spot)
#     return results
#
# def get_filtered_dico():
#     """
#     Fetch and parse all active option chains.
#     Returns dict: { ROOT → ( [localsymbols], spot_price ) }
#     """
#     raw = _fetch_all()
#     results = {}
#     for x in raw:
#         symbols, spot = _filter_options(x)
#         results[x['idx']] = (symbols, spot)
#     return results

# def get_all_filtered():
#     """
#     Fetch and parse all active option chains.
#     Returns dict: { ROOT → ( [localsymbols], spot_price ) }
#     """
#     raw = _fetch_all()
#     results = {}
#     for x in raw:
#         symbols, spot = _filter_options(x)
#         results[x['idx']] = (symbols, spot)
#     return results

def get_url(idx):
    return f"https://cdn.cboe.com/api/global/delayed_quotes/options/{INS[idx]['cdn']}.json"

async def _fetch_all_async():
    results = {}
    cases = [i for i, x in enumerate(INS) if x['sType'] == b'OPT\x00']
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        tasks = [_fetch_one_async(session, i) for i in cases]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
    return [_filter_options(x) for x in responses]

async def _fetch_one_async(session, idx):
    try:
        url= get_url(idx)
        async with session.get(url, timeout=10) as r:
            r.raise_for_status()
            resp = await r.json()
            data = resp['data']
            return {'idx':idx,'root':INS[idx]['root'], 'spot':data['current_price'],'step':INS[idx]['step'],'ops':[x['option'] for x in data['options']]}
    except Exception as e:
        print(f"[CDN] Failed {INS[idx]['root']}: {e}")
        return {'idx':idx, 'root':INS[idx]['root'], 'spot':None,'step':INS[idx]['step'],'ops':None}




def _fetch_all():
    res = []
    # Create a session for connection reuse and set headers
    with requests.Session() as session:
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        for i,x in enumerate(INS):
            if x['sType']!=b'OPT\x00':continue
            resp=_fetch_one(session, i)
            if resp is not None:
                res.append(resp)  # keep cfg with payload
    return [_filter_options(x) for x in res]


def _fetch_one(session, idx):
    try:
        url= get_url(idx)
        with session.get(url, timeout=10) as r:
            r.raise_for_status()
            resp = r.json()
            data = resp['data']
            return {'idx':idx,'root':INS[idx]['root'], 'spot':data['current_price'],'step':INS[idx]['step'],'ops':[x['option'] for x in data['options']]}
    except Exception as e:
        print(f"[CDN] Failed {INS[idx]['root']}: {e}")
        return {'idx':idx, 'root':INS[idx]['root'], 'spot':None,'step':INS[idx]['step'],'ops':None}

def _filter_options(pload):
    px=pload['spot']
    ops=pload['ops']
    idx=pload['idx']
    step=pload['step']
    if px is None:
        print('parsing of cdn failed')
        return [], float('nan')
    lbase = math.floor(px / step) * step
    hbase = math.ceil(px / step) * step
    low = int((lbase - 127 * step) * 1000)
    high = int((hbase + 127 * step) * 1000)
    if ops is None:
        print('parsing of cdn failed')
        return None
    filtered = [x for x in ops
                if (dt.strptime(x[-15:-9], '%y%m%d') - dt.now()).days >= 0
                and (
                        (low <= int(x[-8:]) < high and len(x[:-15]) > 3)  # SPXW
                        or (len(x[:-15]) == 3 and (dt.strptime(x[-15:-9], '%y%m%d') - dt.now()).days <= 90)
                )]
    res=copy.deepcopy(pload)
    res['ops'] = filtered
    return res

#
#
# class Cdn:
#     @staticmethod
#     async def get_all_local_symbols_async():
#         """
#         Fetch and parse all active option chains.
#         Returns dict: { ROOT → ( [localsymbols], spot_price ) }
#         """
#         raw = await Cdn._fetch_all_async()
#         results = {}
#         for root, (payload, cfg) in raw.items():
#             if payload:
#                 symbols, spot = Cdn._parse_payload(payload)
#                 symbols, spot = Cdn._filter_payload(symbols, spot)
#                 results[root] = (symbols, spot, cfg)
#         return results
#
#     #
#     # @staticmethod
#     # def get_all_local_symbols():
#     #     """
#     #     Fetch and parse all active option chains.
#     #     Returns dict: { ROOT → ( [localsymbols], spot_price ) }
#     #     """
#     #     raw = Cdn._fetch_all()
#     #     results = {}
#     #     for idx, (payload, cfg) in raw.items():
#     #         if payload:
#     #             symbols, spot = Cdn._parse_payload(payload)
#     #             symbols, spot = Cdn._filter_payload(symbols, spot)
#     #             results[idx] = (symbols, spot, cfg)
#     #     return results
#
#     @staticmethod
#     def get_url(idx):
#         return f"https://cdn.cboe.com/api/global/delayed_quotes/options/{INS[idx]['cdn']}.json"
#     @staticmethod
#     async def _fetch_all_async():
#         results = {}
#         cases =[i for i,x in enumerate(INS) if x['sType']==b'OPT\x00']
#         async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
#             tasks = [Cdn._fetch_one_async(session, i) for i in cases]
#             responses = await asyncio.gather(*tasks, return_exceptions=True)
#             for idx, data in responses:
#                 if data is not None:
#                     results[idx]= data   # keep cfg with payload
#         return results
#
#     # @staticmethod
#     # def _fetch_all():
#     #     urls = Cdn._get_cdn_urls()  # returns (root, cfg, url)
#     #     results = {}
#     #     # Create a session for connection reuse and set headers
#     #     with requests.Session() as session:
#     #         session.headers.update({"User-Agent": "Mozilla/5.0"})
#     #
#     #         for idx, (root, cfg, url) in urls.items():
#     #             root, cfg, resp = Cdn._fetch_one(session, root, url, cfg)
#     #             if resp is not None:
#     #                 results[idx] = (resp, cfg)  # keep cfg with payload
#     #     return results
#
#     @staticmethod
#     def _parse_payload(payload):
#         """Extract (local_symbols, spot_px) from a CDN payload"""
#         opts, px = [], None
#         d = payload.get("data") if isinstance(payload, dict) else None
#         if not isinstance(d, dict):
#             return [], None
#         px = d.get("current_price", px)
#         raw = d.get("options")
#         if not isinstance(raw, list):
#             return [], px
#         for o in raw:
#             s = o.get("option") if isinstance(o, dict) else None
#             if isinstance(s, str):
#                 opts.append(s)
#         return opts, px
#
#
#     @staticmethod
#     def _filter_payload(opts, px):
#         if px is None:
#             print('parsing of cdn failed')
#             return [], float('nan')
#         step = 5.0
#         lbase = math.floor(px / step) * step
#         hbase = math.ceil(px / step) * step
#         low = int((lbase - 127 * step) * 1000)
#         high = int((hbase + 127 * step) * 1000)
#         if opts is None:
#             print('parsing of cdn failed')
#             return None
#         filtered = [x for x in opts
#                     if (dt.strptime(x[-15:-9], '%y%m%d') - dt.now()).days >= 0
#                     and (low <= int(x[-8:]) < high)
#                     and (
#                         len(x[:-15]) > 3  # SPXW
#                         or (len(x[:-15]) == 3 and (dt.strptime(x[-15:-9], '%y%m%d') - dt.now()).days <= 90)
#                     )]
#         return filtered, float(px) if px is not None else float("nan")
#
#
#     @staticmethod
#     def get_expiries_per_class(ops):
#         sr = sorted(set([x[:-9] for x in ops]))
#         return {k[:-6]: sorted([v[-6:] for v in sr if v[:-6] == k[:-6]]) for k in sr}
#
#     # @staticmethod
#     # def _get_cdn_urls():
#     #     urls={}
#     #     seen = set()
#     #     for x in [(i,y) for i,y in enumerate(INS) if y['sType']==b'OPT\x00' and y['active'] and y['cdn'] not in seen and not seen.add(y['cdn'])]:
#     #         urls[x[0]] = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{x[1]['cdn']}.json"
#     #     return urls
#
#
#     @staticmethod
#     async def _fetch_one_async(session, idx):
#         try:
#             url= Cdn.get_url(idx)
#             async with session.get(url, timeout=10) as r:
#                 r.raise_for_status()
#                 resp = await r.json()
#                 data = resp['data']
#                 return {idx: {'root':INS[idx]['root'], 'spot':data['current_price'],'ops':[x['option'] for x in data['options']]}}
#         except Exception as e:
#             print(f"[CDN] Failed {INS[idx]['root']}: {e}")
#             return idx, {'root':INS[idx]['root'], 'spot':None,'ops':None}
#
#     # @staticmethod
#     # def _fetch_one(session,k, v):
#     #     try:
#     #         with session.get(v[2], timeout=10) as r:
#     #             r.raise_for_status()
#     #             resp = r.json()
#     #             data = resp['data']
#     #             return {k: {'root':v[0], 'cfg':v[1], 'spot':data['current_price'],'ops':[x['option'] for x in data['options']]}}
#     #     except Exception as e:
#     #         print(f"[CDN] Failed {v[0]}: {e}")
#     #         return  {k: {'root':v[0], 'cfg':v[1], 'spot':None,'ops':None}}