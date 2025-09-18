#!/usr/bin/env python3
import asyncio

from cts.cts_cache import _gen_key
from cts.cts_cfg import PERM, FUT
from cts.cts_api import CtsApi

def check_missings(cfg, _cache):
    if not cfg["active"]:
        return
    key = _gen_key(cfg["root"], '', 0, 0, cfg["xch"], cfg["tc"], cfg.get("step", 1))
    if key not in _cache.records:
        _cache.add_record(key, cfg["conid"])
        print(f"[PERM] Added {cfg['root']} {cfg['tc']} -> {cfg['conid']}")
    else:
        print(f"[PERM] Already in cache: {cfg['root']} {cfg['tc']}")


def load_perm_into_cache(_cache):
    """Insert all permanent instruments (PERM) into cache if missing."""
    for cfg in PERM:
        if not cfg["active"]:
            continue
        key = _gen_key(cfg["root"], 0, 0, 0, cfg["xch"], cfg["tc"], cfg.get("step", 1))
        if key not in _cache.records:
            _cache.add_record(key, cfg["conid"])
            print(f"[PERM] Added {cfg['root']} {cfg['tc']} -> {cfg['conid']}")
        else:
            print(f"[PERM] Already in cache: {cfg['root']} {cfg['tc']}")
    _cache.save()


async def load_fut_into_cache(_cache, gateway_port: int = 4012):
    """Insert missing FUT contracts using IB callback (conid only)."""
    api = CtsApi(slot=1)
    api.tws.port = gateway_port
    await api.tws.connect_async()
    try:
        for idx, cfg in enumerate(FUT):
            if not cfg["active"]:
                continue
            base_key = _gen_key(cfg["root"], 0, 0, 0, cfg["xch"], cfg["tc"], cfg.get("step", 1))
            if base_key in _cache.records:
                print(f"[FUT] Already in cache: {cfg['root']} {cfg['tc']} on {cfg['xch']}")
                continue
            print(f"[FUT] Requesting {cfg['root']} {cfg['tc']} on {cfg['xch']}")
            async for conid in api.req_fut(idx):
                key = _gen_key(cfg["root"], 0, 0, 0, cfg["xch"], cfg["tc"], cfg.get("step", 1))
                _cache.add_record(key, conid)
                _cache.save()
    finally:
        await api.tws.close_async()
        print("FUT gateway disconnected")


async def load_opt_into_cache(_cache, gateway_port: int = 4012):
    """Insert missing OPT contracts (CDN + IB)."""
    print("[OPT] Starting option load")
    cdn_data = await Cdn.get_all_local_symbols()
    for root, (symbols, spot, cfg) in cdn_data.items():
        print(f"[OPT] Found {len(symbols)} CDN symbols for {root}, spot={spot:.2f}")
        opt_keys = {sym: CtsCache.parse_cdn_symbol_to_key(sym, cfg) for sym in symbols}
        missings = {sym: key for sym, key in opt_keys.items() if key not in _cache.records}
        print(f"[OPT] {len(missings)} missings to request from IB")
        if not missings:
            continue
        api = CtsApi(slot=1)
        api.tws.port = gateway_port
        try:
            await api.tws.connect_async()
            for i, (sym, key) in enumerate(missings.items()):
                try:
                    conid = await api.req_cboe_opt(sym)  # now returns conid only
                    if conid:
                        _cache.add_record(key, conid)
                        _cache.save()
                        print(f"[OPT] Added {sym} -> {conid}")
                except Exception as e:
                    print(f"[OPT ERROR] {sym}: {e}")
                if (i + 1) % 200 == 0:
                    print(f"[OPT] Progress {i+1}/{len(missings)}")
                await asyncio.sleep(0.2)
        finally:
            await api.tws.close_async()
            print("[OPT] Gateway disconnected")


if __name__ == "__main__":
    cache = HistoCache()
    cache.load()
    load_perm_into_cache(cache)
    asyncio.run(load_fut_into_cache(cache))
    asyncio.run(load_opt_into_cache(cache))
