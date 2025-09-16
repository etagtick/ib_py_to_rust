import asyncio
from cts.cts_cdn import Cdn
from cts.cts_cfg import PERM, FUT, OPT
from cts.cts_hst_cache import HistoCache
from cts.cts_cache import CtsCache
from cts.cts_api import CtsApi


def load_perm_into_cache(cache: HistoCache):
    """
    Ensure all PERM instruments from cts_cfg are present in the cache.
    Uses CtsCache.gen_key for consistency.
    """
    for cfg in PERM:
        if not cfg["active"]:
            continue

        key = CtsCache.gen_key(
            root=cfg["root"],
            expiry_mmddy=0,
            strike=0,
            right=0,
            exchange=cfg["xch"],
            tclass=cfg["tc"],
            step=cfg.get("step", 1),  # default 1 for PERM
        )

        if key not in cache.records:
            cache.add_record(key, cfg["conid"])
            print(f"[PERM] Added {cfg['root']} {cfg['tc']} -> {cfg['conid']}")
        else:
            print(f"[PERM] Already in cache: {cfg['root']} {cfg['tc']}")

    cache.save()

async def load_fut_into_cache(cache: HistoCache, gateway_port: int = 4012):
    """Add missing FUT contracts into cache (request from IB only if not cached)."""
    api = CtsApi(slot=1)
    api.tws.port = gateway_port
    await api.tws.connect_async()

    try:
        for idx, cfg in enumerate(FUT):
            if not cfg["active"]:
                continue

            # build base key (expiry=0, strike=0, right=0)
            base_key = CtsCache.gen_key(
                root=cfg["root"],
                expiry_mmddy=0,
                strike=0,
                right=0,
                exchange=cfg["xch"],
                tclass=cfg["tc"],
                step=cfg.get("step", 1),
            )

            if base_key in cache.records:
                print(f"[FUT] Already in cache: {cfg['root']} {cfg['tc']} on {cfg['xch']}")
                continue  # skip request

            print(f"[FUT] Requesting {cfg['root']} {cfg['tc']} on {cfg['xch']}")

            async for result in api.req_fut(idx):  # req_fut now yields one-by-one
                if not result:
                    continue

                key = CtsCache.gen_key(
                    result[0],  # root
                    result[1],  # expiry
                    result[2],  # strike
                    result[3],  # right
                    result[4],  # exchange
                    result[5],  # tclass
                    step=cfg.get("step", 1),
                )
                conid_binary = result[6]

                if key not in cache.records:
                    cache.add_record(key, conid_binary)
                    cache.save()
                    print(f"[FUT] Added {cfg['root']} {cfg['tc']} -> {conid_binary}")

    finally:
        await api.tws.close_async()
        print("FUT gateway disconnected")

async def load_opt_into_cache2(cache, gateway_port: int = 4012):
    """Load missing OPT contracts into cache (using CDN → IB)."""
    print(f"[OPT] Starting on gateway {gateway_port}")
    cdn_data = await Cdn.get_all_local_symbols()

    # Loop over all configured option roots
    for cfg in [x for x in OPT if x["active"]]:
        root = cfg["root"]

        if root not in cdn_data:
            print(f"[OPT] No CDN data for {root}")
            continue

        symbols, spot, cfg = cdn_data[root]
        print(f"[OPT] Found {len(symbols)} CDN symbols for {root}, spot={spot}")

        # Build keys for all CDN symbols
        opt_keys = [
            CtsCache.gen_key(**CtsCache.parse_cdn_symbol_to_key(sym, cfg))
            for sym in symbols
        ]
        missings = [sym for sym, key in zip(symbols, opt_keys) if key not in cache.records]
        print(f"[OPT] {len(missings)} symbols missing in cache")

        if not missings:
            continue

        api = CtsApi(slot=1)
        api.tws.port = gateway_port
        await api.tws.connect_async()
        try:
            for i, sym in enumerate(missings):
                try:
                    result = await api.req_cboe_opt(sym)
                    if result:
                        root, expiry, strike, right, xch, tclass, conid = result
                        key = CtsCache.gen_key(
                            root=root,
                            expiry_mmddy=expiry,
                            strike=strike,
                            right=right,
                            exchange=xch,
                            tclass=tclass,
                            step=cfg.get("step", 1),
                        )
                        cache.add_record(key, conid)
                        cache.save()
                        print(f"[OPT] Added {sym} -> {conid}")
                except Exception as e:
                    print(f"[OPT ERROR] {sym}: {e}")

                if (i + 1) % 200 == 0:
                    print(f"[OPT] Progress {i+1}/{len(missings)}")

                await asyncio.sleep(0.2)  # throttle
        finally:
            await api.tws.close_async()
            print(f"[OPT] Gateway {gateway_port} disconnected")

async def load_opt_into_cache(cache: HistoCache, gateway_port: int = 4012):
    print("[OPT] Starting option load")
    cdn_data = await Cdn.get_all_local_symbols()

    for root, (symbols, spot, cfg) in cdn_data.items():
        print(f"[OPT] Found {len(symbols)} CDN symbols for {root}, spot={spot:.2f}")

        # Build keys from CDN
        opt_keys = {sym: CtsCache.gen_key(**CtsCache.parse_cdn_symbol_to_key(sym, cfg)) for sym in symbols}

        # Filter only missings
        missings = {sym: key for sym, key in opt_keys.items() if key not in cache.records}
        print(f"[OPT] {len(missings)} missings to request from IB")

        if not missings:
            continue

        api = CtsApi(slot=1)
        api.tws.port = gateway_port
        try:
            await api.tws.connect_async()
            for i, (sym, key) in enumerate(missings.items()):
                try:
                    result = await api.req_cboe_opt(sym)
                    if not result:
                        continue

                    # Rebuild key from IB callback
                    ib_key = CtsCache.gen_key(
                        result[0], result[1], result[2],
                        result[3], result[4], result[5],
                        cfg["step"]
                    )
                    conid_binary = result[6]

                    if ib_key == key:
                        cache.add_record(ib_key, conid_binary)
                        cache.save()
                    else:
                        print(f"[OPT WARNING] Key mismatch {sym}: CDN={key.hex()} IB={ib_key.hex()}")
                        # Use IB’s key as ground truth
                        if ib_key not in cache.records:
                            cache.add_record(ib_key, conid_binary)
                            cache.save()

                    # # Compare keys
                    # if ib_key != key:
                    #     print(f"[OPT WARNING] Key mismatch for {sym}: CDN={key.hex()} IB={ib_key.hex()}")
                    #     continue
                    #
                    # # Add or overwrite in cache
                    # if key in cache.records and cache.records[key] != conid_binary:
                    #     print(f"[OPT] Overwriting {key.hex()} with new conid {conid_binary}")
                    # else:
                    #     print(f"[OPT] Adding {key.hex()} -> {conid_binary}")
                    #
                    # cache.add_record(key, conid_binary)
                    # cache.save()

                except Exception as e:
                    print(f"[OPT ERROR] {sym}: {e}")

                if (i + 1) % 100 == 0:
                    print(f"[OPT] Progress {i+1}/{len(missings)}")

                await asyncio.sleep(0.2)  # throttle

        finally:
            await api.tws.close_async()
            print("[OPT] Gateway disconnected")


if __name__ == "__main__":
    mycache = HistoCache()
    mycache.load()
    load_perm_into_cache(mycache)
    asyncio.run(load_fut_into_cache(mycache))
    asyncio.run(load_opt_into_cache(mycache))


