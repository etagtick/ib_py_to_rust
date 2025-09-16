#!/usr/bin/env python3
import sys

from core.core_util import E_EMPTY
from cts.cts_cache import CtsCache
from cts.cts_hst_cache import HistoCache


def get_key_from_prms(root, expiry, strike, right, exchange, tclass, step):
    if right=='': right=0
    if strike == '': strike = 0
    if expiry == '': expiry = 0
    if tclass == '': tclass = E_EMPTY
    if step == '': step = 1
    return CtsCache.gen_key(root, expiry, strike, right, exchange, tclass, step)

def print_conid(cache, root, expiry, strike, right, exchange, tclass, step):
    key = get_key_from_prms(root, expiry, strike, right, exchange, tclass, step)
    conid = cache.get_conid(key)
    if conid:
        print(f"Conid: {conid}")
    else:
        print("Not found in cache")

def main():

    cache = HistoCache()
    if not cache.load():
        print("Cache not found or empty")
        sys.exit(1)

    print_conid(cache,b'EUR\x00', '', '', '', b'IDEALPRO\x00', b'EUR.USD\x00', '')

    print_conid(cache,b'SPX\x00','','','',b'CBOE\x00','','')

    print_conid(cache,b'ES\x00', 125, 0,0, b'CME\x00', b'\x00', 1)


    print_conid(cache, b'SPX\x00', 0, 0, 0, b'CBOE\x00', b'\x00', 1)

if __name__ == "__main__":
    main()
