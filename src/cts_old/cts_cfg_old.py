# cts_cfg.py
import struct
from datetime import datetime as dt
from typing import Tuple


# Pre-encoded binary chunks for contract details request

from core.core_util import encode_field, EMPTY_FIELD


class CtsChunks:
    # Message header chunks
    MSG_CONTRACT_DETAILS = b"9\x00"  # msgId
    VERSION_8 = b"8\x00"  # version


    INCLUDE_EXPIRED_FALSE = b"0\x00"
    INCLUDE_EXPIRED_TRUE = b"1\x00"

    # Option rights
    RIGHT_CALL = b"C\x00"
    RIGHT_PUT = b"P\x00"

    # Root chunks
    SPX = b"SPX\x00"
    VIX = b"VIX\x00"
    SPY = b"SPY\x00"
    EUR = b"EUR\x00"
    GBP = b"GBP\x00"
    JPY = b"JPY\x00"
    USD = b"USD\x00"
    ES = b"ES\x00"
    NQ = b"NQ\x00"
    CL = b"CL\x00"
    GC = b"GC\x00"
    @staticmethod
    def get_root_chunk(root_sym):
        """Get pre-encoded currency chunk"""
        root_map = {
            'SPX': CtsChunks.SPX,
            'VIX': CtsChunks.VIX,
            'SPY': CtsChunks.SPY,
            'EUR': CtsChunks.EUR,
            'GBP': CtsChunks.GBP,
            'JPY': CtsChunks.JPY,
            'USD': CtsChunks.USD,
            'ES': CtsChunks.ES,
            'NQ': CtsChunks.NQ,
            'CL': CtsChunks.CL,
            'GC': CtsChunks.GC,
        }
        return root_map.get(root_sym, encode_field(root_sym))

    # TClass chunks
    SPXW = b"SPX\x00"
    VX = b"VX\x00"
    E6 = b"6E\x00"
    B6 = b"6B\x00"
    A6 = b"6A\x00"
    C6 = b"6C\x00"
    J6 = b"6J\x00"
    N6 = b"6N\x00"
    S6 = b"6S\x00"
    EURUSD = b"EUR.USD\x00"
    GBPUSD = b"GBP.USD\x00"
    AUDUSD = b"AUD.USD\x00"
    USDCAD = b"USD.CAD\x00"
    USDJPY = b"USD.JPY\x00"
    NZDUSD = b"NZD,USD\x00"
    USDCHF = b"USD.CHF\x00"
    @staticmethod
    def get_tclass_chunk(trd_class):
        """Get pre-encoded currency chunk"""
        tclass_map = {
            'SPXW': CtsChunks.SPXW,
            'VX': CtsChunks.VX,
            'E6': CtsChunks.E6,
            'B6': CtsChunks.B6,
            'A6': CtsChunks.A6,
            'C6': CtsChunks.C6,
            'J6': CtsChunks.J6,
            'N6': CtsChunks.N6,
            'S6': CtsChunks.S6,
            'EURUSD': CtsChunks.EURUSD,
            'GBPUSD': CtsChunks.GBPUSD,
            'AUDUSD': CtsChunks.AUDUSD,
            'USDCAD': CtsChunks.USDCAD,
            'USDJPY': CtsChunks.USDJPY,
            'NZDUSD': CtsChunks.NZDUSD,
            'USDCHF': CtsChunks.USDCHF,
        }
        return tclass_map.get(trd_class, encode_field(trd_class))

    # Currency chunks
    CUR_AUD = b"AUD\x00"
    CUR_CAD = b"CAD\x00"
    CUR_GBP = b"GBP\x00"
    CUR_EUR = b"EUR\x00"
    CUR_USD = b"USD\x00"
    CUR_JPY = b"JPY\x00"
    @staticmethod
    def get_currency_chunk(cur):
        """Get pre-encoded currency chunk"""
        cur_map = {
            'AUD': CtsChunks.CUR_AUD,
            'CAD': CtsChunks.CUR_CAD,
            'GBP': CtsChunks.CUR_GBP,
            'EUR': CtsChunks.CUR_EUR,
            'USD': CtsChunks.CUR_USD,
            'JPY': CtsChunks.CUR_JPY,
        }
        return cur_map.get(cur, encode_field(cur))

    # Security type chunks
    SEC_STK = b"STK\x00"
    SEC_CASH = b"CASH\x00"
    SEC_FUT = b"FUT\x00"
    SEC_IND = b"IND\x00"
    SEC_OPT = b"OPT\x00"
    SEC_FOP = b"FOP\x00"
    @staticmethod
    def get_sectype_chunk(sectype):
        """Get pre-encoded security type chunk"""
        sectype_map = {
            'STK': CtsChunks.SEC_STK,
            'IND': CtsChunks.SEC_IND,
            'FUT': CtsChunks.SEC_FUT,
            'CASH': CtsChunks.SEC_CASH,
            'OPT': CtsChunks.SEC_OPT,
            'FOP': CtsChunks.SEC_FOP,
        }
        return sectype_map.get(sectype, encode_field(sectype))


    # Exchange chunks (common ones)
    EXCH_CFE = b"CFE\x00"
    EXCH_ARCA = b"ARCA\x00"
    EXCH_NASDAQ = b"NASDAQ\x00"
    EXCH_NYMEX = b"NYMEX\x00"
    EXCH_CME = b"CME\x00"
    EXCH_SMART = b"SMART\x00"
    EXCH_CBOE = b"CBOE\x00"
    EXCH_IDEALPRO = b"IDEALPRO\x00"
    @staticmethod
    def get_exchange_chunk(exchange):
        """Get pre-encoded exchange chunk"""
        exchange_map = {
            'CFE': CtsChunks.EXCH_CFE,
            'NYMEX': CtsChunks.EXCH_NYMEX,
            'CME': CtsChunks.EXCH_CME,
            'SMART': CtsChunks.EXCH_SMART,
            'CBOE': CtsChunks.EXCH_CBOE,
            'ARCA': CtsChunks.EXCH_ARCA,
            'NASDAQ': CtsChunks.EXCH_NASDAQ,
            'IDEALPRO': CtsChunks.EXCH_IDEALPRO,
        }
        return exchange_map.get(exchange, encode_field(exchange))

    RT_BAR_SIZE = b"5\x00"



QUARTERLY = 2
MONTHLY = 1



ROOT_PATH = "C:/Users/em/repos/python/ib_py_to_rust/"
DAILY_CACHE = 'daily_cache.bin'
HISTO_CACHE = 'historical_contracts.bin'

RECORD_FMT = "<BBBBI f I"  # root, tclass, right, exch, expiry, strike, conid
RECORD_SIZE = struct.calcsize(RECORD_FMT)  # 16 bytes

RIGHT = ['NONE','C','P','F']
sType= ['IND','STK','CASH','FUT','OPT','FOP']
CUR = ['USD','EUR','GBP','AUD','NZD','CAD','CHF','JPY']
wDay=['MO','TU','WE','TH','FR']
month_codes = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
MTH = ['NONE', 'F', 'G', 'H', 'J','K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']

PORTS = [4002, 4012]
frequency = ['NONE','M','Q']
root=[('SPX',('', 'SPX','SPXW')),('VIX',('','VIX','VX')),('SPY',('','SPY')),('EUR',('','EUR.USD','EUR.JPY','6E')),('GBP',('','GBP.USD','GBP.JPY','6B')),('ES',('','ES')),('CL',('','CL')),('JPY',('','6J')),('USD',('', 'USD.JPY'))]
exch = ['CBOE','ARCA','IDEALPRO','CFE','CME','NYMEX','COMEX','GLOBEX','SMART']

def get_tc_idx(rt,cls):
    return [x[1] for x in root][[y[0] for y in root].index(rt)].index(cls)
def get_rt_idx(rt):
    return [x[0] for x in root].index(rt)


# PERM = [
#     {'root':get_rt_idx('SPX')  ,'tclass':get_tc_idx('SPX','')       , 'exch': exch.index('CBOE')     ,'conid': 416904  , 'port':0,'active': True},
#     {'root':get_rt_idx('VIX')  ,'tclass':get_tc_idx('SPX','')       , 'exch': exch.index('CBOE')     ,'conid': 13455763, 'port':0,'active': True},
#     {'root':get_rt_idx('SPY')  ,'tclass':get_tc_idx('SPY','')       , 'exch': exch.index('ARCA')     ,'conid': 756733  , 'port':0,'active': True},
#     {'root':get_rt_idx('EUR')  ,'tclass':get_tc_idx('EUR','EUR.USD'), 'exch': exch.index('IDEALPRO') ,'conid': 12087792, 'port':0,'active': True},
#     {'root':get_rt_idx('GBP')  ,'tclass':get_tc_idx('GBP','GBP.USD'), 'exch': exch.index('IDEALPRO') ,'conid': 12087797, 'port':0,'active': True},
#     {'root':get_rt_idx('USD')  ,'tclass':get_tc_idx('USD','USD.JPY'), 'exch': exch.index('IDEALPRO') ,'conid': 15016059, 'port':0,'active': True},
# ]
#
# FUT = [
#     {'root':get_rt_idx('VIX')   ,'tclass':get_tc_idx('VIX','VX')    ,'exch': exch.index('CFE'),     'freq': 1, 'count' : 4, 'port':1, 'active': False},
#     {'root':get_rt_idx('EUR')   ,'tclass':get_tc_idx('EUR','6E')    ,'exch': exch.index('CME'),     'freq': 2, 'count' : 2, 'port':0, 'active': True},
#     {'root':get_rt_idx('GBP')   ,'tclass':get_tc_idx('GBP','6B')    ,'exch': exch.index('CME'),     'freq': 2, 'count' : 2, 'port':0, 'active': False},
#     {'root':get_rt_idx('JPY')   ,'tclass':get_tc_idx('JPY','6J')    ,'exch': exch.index('CME'),     'freq': 2, 'count' : 2, 'port':0, 'active': False},
#     {'root':get_rt_idx('ES')    ,'tclass':get_tc_idx('ES','')     ,'exch': exch.index('CME'),     'freq': 2, 'count' : 2, 'port':0, 'active': True},
#     {'root':get_rt_idx('CL')    ,'tclass':get_tc_idx('CL','')     ,'exch': exch.index('NYMEX'),   'freq': 1, 'count' : 4, 'port':0, 'active': True}
# ]
#
# OPT = [
#     {'root':get_rt_idx('SPX')   ,'tclass':get_tc_idx('SPX','SPX')   ,'exch': exch.index('CBOE'),   'cdn':'_SPX','base_expiry_index': 50, 'step' : 5, 'active': True},
#     {'root':get_rt_idx('SPX')   ,'tclass':get_tc_idx('SPX','SPXW')  ,'exch': exch.index('CBOE'),   'cdn':'_SPX', 'step': 5, 'active': True},
#     {'root':get_rt_idx('SPY')   ,'tclass':get_tc_idx('SPY','')      ,'exch': exch.index('CBOE'),   'cdn': 'SPY','base_expiry_index': None, 'step' : 5, 'active': False},
# ]
#
# FOP = [
#     {'root':get_rt_idx('EUR')   ,'tclass':get_tc_idx('EUR','')      ,'exch': exch.index('CME'),    'cdn': ''   ,'days':'OUEU', 'active': False},
#     {'root':get_rt_idx('VIX')   ,'tclass':get_tc_idx('VIX','')      ,'exch': exch.index('CFE'),    'cdn': ''   ,'days':'BGGB', 'suffix':'SB', 'active': False},
#     {'root':get_rt_idx('CL')    ,'tclass':get_tc_idx('CL','')       ,'exch': exch.index('CME'),    'cdn': ''   ,'days':'JJJJ', 'suffix': 'SB', 'active': False},
#     {'root':get_rt_idx('ES')    ,'tclass':get_tc_idx('ES','')       ,'exch': exch.index('CME'),    'cdn': ''   ,'days':'ABCD', 'active': False},
# ]

PERM = [
    {'root':CtsChunks.SPX  ,'tclass':EMPTY_FIELD     , 'exch': CtsChunks.EXCH_CBOE     ,'conid': b"416904\x00"  , 'port':0,'active': True},
    {'root':CtsChunks.VIX  ,'tclass':EMPTY_FIELD     , 'exch': CtsChunks.EXCH_CBOE     ,'conid': b"13455763\x00", 'port':0,'active': True},
    {'root':CtsChunks.SPY  ,'tclass':EMPTY_FIELD     , 'exch': CtsChunks.EXCH_ARCA     ,'conid': b"756733\x00"  , 'port':0,'active': True},
    {'root':CtsChunks.EUR  ,'tclass':CtsChunks.EURUSD, 'exch': CtsChunks.EXCH_IDEALPRO ,'conid': b"12087792\x00", 'port':0,'active': True},
    {'root':CtsChunks.GBP  ,'tclass':CtsChunks.GBPUSD, 'exch': CtsChunks.EXCH_IDEALPRO ,'conid': b"12087797\x00", 'port':0,'active': True},
    {'root':CtsChunks.USD  ,'tclass':CtsChunks.USDJPY, 'exch': CtsChunks.EXCH_IDEALPRO ,'conid': b"15016059\x00", 'port':0,'active': True},
]

FUT = [
    {'root':CtsChunks.VIX   ,'tclass':CtsChunks.VX   ,'exch': CtsChunks.EXCH_CFE,   'freq': MONTHLY, 'count' : 4, 'port':1, 'active': True},
    {'root':CtsChunks.EUR   ,'tclass':CtsChunks.E6   ,'exch': CtsChunks.EXCH_CME,   'freq': QUARTERLY, 'count' : 2, 'port':0, 'active': True},
    {'root':CtsChunks.GBP   ,'tclass':CtsChunks.B6   ,'exch': CtsChunks.EXCH_CME,   'freq': QUARTERLY, 'count' : 2, 'port':0, 'active': False},
    {'root':CtsChunks.JPY   ,'tclass':CtsChunks.J6   ,'exch': CtsChunks.EXCH_CME,   'freq': QUARTERLY, 'count' : 2, 'port':0, 'active': False},
    {'root':CtsChunks.ES    ,'tclass':EMPTY_FIELD    ,'exch': CtsChunks.EXCH_CME,   'freq': QUARTERLY, 'count' : 2, 'port':0, 'active': True},
    {'root':CtsChunks.CL    ,'tclass':EMPTY_FIELD    ,'exch': CtsChunks.EXCH_NYMEX, 'freq': MONTHLY, 'count' : 4, 'port':0, 'active': True}
]

OPT = [
    {'root':CtsChunks.SPX   ,'tclass':CtsChunks.SPX   ,'exch': CtsChunks.EXCH_CBOE,   'cdn':'_SPX','base_expiry_index': 50, 'step' : 5, 'active': True},
    {'root':CtsChunks.SPX   ,'tclass':CtsChunks.SPXW  ,'exch': CtsChunks.EXCH_CBOE,   'cdn':'_SPX', 'step': 5, 'active': True},
    {'root':CtsChunks.SPY   ,'tclass':EMPTY_FIELD     ,'exch': CtsChunks.EXCH_CBOE,   'cdn': 'SPY','base_expiry_index': None, 'step' : 5, 'active': False},
]

FOP = [
    {'root':CtsChunks.EUR   ,'tclass':EMPTY_FIELD      ,'exch': CtsChunks.EXCH_CME,    'cdn': ''   ,'days':'OUEU', 'active': False},
    {'root':CtsChunks.VIX   ,'tclass':EMPTY_FIELD      ,'exch': CtsChunks.EXCH_CFE,    'cdn': ''   ,'days':'BGGB', 'suffix':'SB', 'active': False},
    {'root':CtsChunks.CL    ,'tclass':EMPTY_FIELD      ,'exch': CtsChunks.EXCH_CME,    'cdn': ''   ,'days':'JJJJ', 'suffix': 'SB', 'active': False},
    {'root':CtsChunks.ES    ,'tclass':EMPTY_FIELD      ,'exch': CtsChunks.EXCH_CME,    'cdn': ''   ,'days':'ABCD', 'active': False},
]


def root_name(idx): return root[idx][0]
def tclass_name(root_idx, tclass_idx): return root[root_idx][1][tclass_idx]
def exch_name(idx): return exch[idx]
def cur_name(idx): return CUR[idx]

DEFAULT_CUR   = CUR.index("USD")
DEFAULT_CASHX = exch.index("IDEALPRO")
DEFAULT_OPTX  = exch.index("CBOE")


# Cache settings (global)
CACHE_GLOBAL = {
    "array_size": 65536,
    "strike_range": 127,  # Always ±127 for uint8 encoding
    "request_delay_sec": 0.005
}

# Network settings
CLIENT_CONFIG = {
    "version_min": 100,
    "version_max": 157,
    "base_client_ids": {1: 100, 2: 200, 3: 300},  # CTS, MKT, ORD topics
    "default_ports": [4002, 4012],
    "retry_attempts": 3
}

def get_day_of_week_occurrence(date_str: str) -> Tuple[str, int]:
    DAY_MAP = ('M', 'T', 'W', 'S', 'F', 'X', 'Z')
    parsed_date = dt.strptime(date_str, "%Y%m%d")
    day_char = DAY_MAP[parsed_date.weekday()]
    # Calculate the occurrence of this weekday in the month.
    # Integer division of the (day-1) by 7 gives the number of full weeks
    # that have passed. Adding 1 gives the occurrence count.
    # Example: Day 1-7 -> (d-1)//7 = 0 -> 1st occurrence
    #          Day 8-14 -> (d-1)//7 = 1 -> 2nd occurrence
    occurrence = (parsed_date.day - 1) // 7 + 1
    return day_char, occurrence

def gen_fut_exp(freq, count):
    """
    Generate forward future expiries (YYYYMM) based on frequency and count.
    Checks current month validity.
    """
    now = dt.now()
    year, month = now.year, now.month
    expiries = []

    if freq == MONTHLY:  # Monthly
        step = 1
        #month +=1
    elif freq == QUARTERLY:  # Quarterly
        step = 3
        # align to next quarterly month
        while month % 3 != 0:
            month += 1
            if month > 12:
                month = 1
                year += 1
    else:
        return []

    for _ in range(count):
        expiries.append(int(f'{year}{month:02d}'))
        month += step
        if month > 12:
            month -= 12
            year += 1
    return expiries