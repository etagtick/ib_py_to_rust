#!/usr/bin/env python3
import asyncio
from cts.cts_cache import CtsCache
from cts.cts_cdn import Cdn
from cts.cts_hst_cache import HistoCache
from cts.cts_cfg import PERM, FUT
from cts.cts_api import CtsApi


async def fetch_and_store_perm(cache: HistoCache):
    """Fetch missing PERM instruments and add them to cache."""
    perm_keys = []
    for cfg in PERM:
        if not cfg["active"]:
            continue
        key = CtsCache.gen_key(cfg["root"], 0, 0, 0, cfg["xch"], cfg["tc"], cfg.get("step", 1))
        perm_keys.append((key, cfg))

    missing = cache.find_missing_perm([k for k, _ in perm_keys])
    for key, cfg in perm_keys:
        if key in missing:
            # conid comes from config itself
            conid_binary = cfg["conid"]
            cache.add_record(key, conid_binary)
    print(f"[PERM] Added {len(missing)} missing instruments")


async def fetch_and_store_fut(cache: HistoCache):
    """Fetch missing FUT instruments and add them to cache."""
    fut_keys = []
    for cfg in FUT:
        if not cfg["active"]:
            continue
        key = CtsCache.gen_key(cfg["root"], 0, 0, 0, cfg["xch"], cfg["tc"], cfg.get("step", 1))
        fut_keys.append((key, cfg))

    missing = cache.find_missing_fut([k for k, _ in fut_keys])
    for key, cfg in fut_keys:
        if key in missing:
            # Futures need IB request for conid
            api = CtsApi(slot=1)
            await api.tws.connect_async()
            try:
                contract = await api.req_fut(cfg["symbol"], cfg["xch"])
                conid_binary = contract[6]
                cache.add_record(key, conid_binary)
            finally:
                await api.tws.close_async()
    print(f"[FUT] Added {len(missing)} missing futures")


async def fetch_and_store_opt(cache: HistoCache, gateway_port: int):
    """Fetch missing OPT contracts from CDN+IB and add them to cache."""
    cdn_data = await Cdn.get_all_local_symbols()
    for root, (symbols, spot, cfg) in cdn_data.items():
        opt_keys = []
        for sym in symbols:
            parsed = CtsCache.parse_cdn_symbol_to_key(sym, cfg)
            key = CtsCache.gen_key(**parsed)
            opt_keys.append((key, sym, cfg))

        missing = cache.find_missing_opt([k for k, _, _ in opt_keys])
        print(f"[OPT] Root {root}: {len(missing)} contracts missing")

        api = CtsApi(slot=1)
        api.tws.port = gateway_port
        await api.tws.connect_async()
        try:
            for key, sym, ocfg in opt_keys:
                if key in missing:
                    result = await api.req_cboe_opt(sym)
                    if result:
                        conid_binary = result[6]
                        cache.add_record(key, conid_binary)
                        cache.save()
        finally:
            await api.tws.close_async()


async def main():
    print("=== Testing Cache Build ===")

    cache = HistoCache()
    cache.load()

    await fetch_and_store_perm(cache)
    await fetch_and_store_fut(cache)
    await fetch_and_store_opt(cache, gateway_port=4012)

    cache.save()


if __name__ == "__main__":
    asyncio.run(main())
