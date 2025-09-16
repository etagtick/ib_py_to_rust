# cts_cfg.py
from datetime import datetime as dt
from typing import Tuple
from core.core_util import E_EMPTY

# 8-byte key structure (little-endian)


#RECORD_FMT = "<BBBBI f I"  # root, tclass, right, exch, expiry, strike, conid ???
#RECORD_SIZE = struct.calcsize(RECORD_FMT)  # 16 bytes


QUARTERLY = 2
MONTHLY = 1

ROOT_PATH = "C:/Users/em/repos/python/ib_py_to_rust/"
DAILY_CACHE = 'daily_cache.bin'
HISTO_CACHE = 'historical_contracts.bin'


RIGHT = ['NONE','C','P','F']
wDay=['MO','TU','WE','TH','FR']
month_codes = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}
MTH = ['NONE', 'F', 'G', 'H', 'J','K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']


PORTS = [4012, 4022]

E_CALL = b'C\x00'
E_PUT = b'P\x00'
RIGHTS = [E_EMPTY,E_CALL,E_PUT]

# Static arrays for index mapping

E_MTH_F = b"01\x00"
E_MTH_G = b"02\x00"
E_MTH_H = b"03\x00"
E_MTH_J = b"04\x00"
E_MTH_K = b"05\x00"
E_MTH_M = b"06\x00"
E_MTH_N = b"07\x00"
E_MTH_Q = b"08\x00"
E_MTH_U = b"09\x00"
E_MTH_V = b"10\x00"
E_MTH_X = b"11\x00"
E_MTH_Z = b"12\x00"
MONTHS=[E_MTH_F,E_MTH_G,E_MTH_H,E_MTH_J,E_MTH_K,E_MTH_M,E_MTH_N,E_MTH_Q,E_MTH_U,E_MTH_V,E_MTH_X,E_MTH_Z]

LS_MT=[b"F",b"G",b"H",b"J",b"K",b"M",b"N",b"Q",b"U",b"V",b"X",b"Z"]
LS_YR=[b"5\x00",b"6\x00",b"7\x00",b"8\x00",b"9\x00",b"0\x00"]

E_CUR_AUD = b"AUD\x00"
E_CUR_CAD = b"CAD\x00"
E_CUR_GBP = b"GBP\x00"
E_CUR_EUR = b"EUR\x00"
E_CUR_USD = b"USD\x00"
E_CUR_JPY = b"JPY\x00"
CURS=[E_CUR_AUD,E_CUR_CAD,E_CUR_GBP,E_CUR_EUR,E_CUR_USD,E_CUR_JPY]

E_SEC_STK = b"STK\x00"
E_SEC_CASH = b"CASH\x00"
E_SEC_FUT = b"FUT\x00"
E_SEC_IND = b"IND\x00"
E_SEC_OPT = b"OPT\x00"
E_SEC_FOP = b"FOP\x00"
TYPES = [E_SEC_STK,E_SEC_CASH,E_SEC_FUT,E_SEC_IND,E_SEC_OPT,E_SEC_FOP]
# Root chunks
E_RT_SPX = b"SPX\x00"
E_RT_VIX = b"VIX\x00"
E_RT_SPY = b"SPY\x00"
E_RT_EUR = b"EUR\x00"
E_RT_GBP = b"GBP\x00"
E_RT_JPY = b"JPY\x00"
E_RT_USD = b"USD\x00"
E_RT_ES = b"ES\x00"
E_RT_NQ = b"NQ\x00"
E_RT_CL = b"CL\x00"
E_RT_GC = b"GC\x00"
ROOTS = [E_RT_SPX,E_RT_VIX,E_RT_SPY,E_RT_EUR,E_RT_GBP,E_RT_JPY,E_RT_USD,E_RT_ES,E_RT_NQ,E_RT_CL,E_RT_GC]

E_XCH_CFE = b"CFE\x00"
E_XCH_ARCA = b"ARCA\x00"
E_XCH_NASDAQ = b"NASDAQ\x00"
E_XCH_NYMEX = b"NYMEX\x00"
E_XCH_CME = b"CME\x00"
E_XCH_SMART = b"SMART\x00"
E_XCH_CBOE = b"CBOE\x00"
E_XCH_IDEALPRO = b"IDEALPRO\x00"
EXCHANGES = [E_XCH_CFE,E_XCH_ARCA,E_XCH_NASDAQ,E_XCH_NYMEX,E_XCH_CME,E_XCH_SMART,E_XCH_CBOE,E_XCH_IDEALPRO]

E_TC_SPX = b"SPX\x00"
E_TC_SPXW = b"SPXW\x00"
E_TC_VX = b"VX\x00"
E_TC_6E = b"6E\x00"
E_TC_6B = b"6B\x00"
E_TC_6A = b"6A\x00"
E_TC_6C = b"6C\x00"
E_TC_6J = b"6J\x00"
E_TC_6N = b"6N\x00"
E_TC_6S = b"6S\x00"
E_TC_EURUSD = b"EUR.USD\x00"
E_TC_GBPUSD = b"GBP.USD\x00"
E_TC_AUDUSD = b"AUD.USD\x00"
E_TC_USDCAD = b"USD.CAD\x00"
E_TC_USDJPY = b"USD.JPY\x00"
E_TC_NZDUSD = b"NZD,USD\x00"
E_TC_USDCHF = b"USD.CHF\x00"
E_TC_SPY = b"SPY\x00"
TCLASSES = [E_TC_SPX,E_TC_SPXW,E_TC_VX,E_TC_6E,E_TC_6B,E_TC_6A,E_TC_6C,E_TC_6J ,E_TC_6N,E_TC_6S,E_TC_EURUSD,E_TC_GBPUSD
    ,E_TC_AUDUSD,E_TC_USDCAD,E_TC_USDJPY,E_TC_NZDUSD,E_TC_USDCHF,E_TC_SPY]

E_MUL_50 = b"50\x00"
E_MUL_100 = b"100\x00"
E_MUL_1000 = b"1000\x00"
E_MUL_100K = b"100000\x00"
E_MUL_125K = b"125000\x00"
E_MUL_62K = b"62500\x00"
E_MUL_6J = b"12500000\x00"
MULTIPLIERS = [E_MUL_50,E_MUL_100,E_MUL_1000,E_MUL_100K,E_MUL_125K,E_MUL_62K,E_MUL_6J]

INS = [
    {'sType': E_SEC_IND,    'root': E_RT_SPX,   'tc': E_EMPTY,      'xch': E_XCH_CBOE,      'conid': b"416904\x00",     'port': 0,  'active': True},
    {'sType': E_SEC_IND,    'root': E_RT_VIX,   'tc': E_EMPTY,      'xch': E_XCH_CBOE,      'conid': b"13455763\x00",   'port': 0,  'active': True},
    {'sType': E_SEC_STK,    'root': E_RT_SPY,   'tc': E_EMPTY,      'xch': E_XCH_ARCA,      'conid': b"756733\x00",     'port': 0,  'active': True},
    {'sType': E_SEC_CASH,   'root': E_RT_EUR,   'tc': E_TC_EURUSD,  'xch': E_XCH_IDEALPRO,  'conid': b"12087792\x00",   'port': 0,  'active': True},
    {'sType': E_SEC_CASH,   'root': E_RT_GBP,   'tc': E_TC_GBPUSD,  'xch': E_XCH_IDEALPRO,  'conid': b"12087797\x00",   'port': 0,  'active': True},
    {'sType': E_SEC_CASH,   'root': E_RT_USD,   'tc': E_TC_USDJPY,  'xch': E_XCH_IDEALPRO,  'conid': b"15016059\x00",   'port': 0,  'active': True},
    {'sType': E_SEC_FUT,    'root': E_RT_VIX,   'tc': E_TC_VX,      'xch': E_XCH_CFE,   'mul': E_MUL_1000,  'freq': MONTHLY,    'count': 4, 'port': 1,     'active': True},
    {'sType': E_SEC_FUT,    'root': E_RT_EUR,   'tc': E_TC_6E,      'xch': E_XCH_CME,   'mul': E_MUL_125K,  'freq': QUARTERLY,  'count': 2, 'port': 0,     'active': True},
    {'sType': E_SEC_FUT,    'root': E_RT_GBP,   'tc': E_TC_6B,      'xch': E_XCH_CME,   'mul': E_MUL_62K,   'freq': QUARTERLY,  'count': 2, 'port': 0,     'active': True},
    {'sType': E_SEC_FUT,    'root': E_RT_JPY,   'tc': E_TC_6J,      'xch': E_XCH_CME,   'mul': E_MUL_6J,    'freq': QUARTERLY,  'count': 2, 'port': 0,     'active': True},
    {'sType': E_SEC_FUT,    'root': E_RT_ES,    'tc': E_EMPTY,      'xch': E_XCH_CME,   'mul': E_MUL_50,    'freq': QUARTERLY,  'count': 2, 'port': 0,     'active': True},
    {'sType': E_SEC_FUT,    'root': E_RT_CL,    'tc': E_EMPTY,      'xch': E_XCH_NYMEX, 'mul': E_MUL_1000,  'freq': MONTHLY,    'count': 4, 'port': 0,     'active': True},
    {'sType': E_SEC_OPT,    'root': E_RT_SPX,   'tc': E_TC_SPXW,    'xch': E_XCH_CBOE,  'mul': E_MUL_100,   'cdn': '_SPX',  'step': 5,  'base_expiry_index': 50,    'other_tc': E_TC_SPX, 'active': True},
    {'sType': E_SEC_OPT,    'root': E_RT_SPY,   'tc': E_EMPTY,      'xch': E_XCH_CBOE,  'mul': E_EMPTY,     'cdn': 'SPY',   'step': 1,  'base_expiry_index': None,  'active': True},
    {'sType': E_SEC_FOP,    'root': E_RT_EUR,   'tc': E_EMPTY,      'xch': E_XCH_CME,   'mul': E_MUL_125K,  'cdn': '',      'step': 0.0025, 'days': 'OUEU',     'active': True},
    {'sType': E_SEC_FOP,    'root': E_RT_VIX,   'tc': E_EMPTY,      'xch': E_XCH_CFE,   'mul': E_MUL_1000,  'cdn': '',      'step': 0.5,    'days': 'BGGB',     'suffix': 'SB', 'active': True},
    {'sType': E_SEC_FOP,    'root': E_RT_JPY,   'tc': E_EMPTY,      'xch': E_XCH_CME,   'mul': E_MUL_1000,  'cdn': '',      'step': 0.25,   'days': 'JJJJ',     'suffix': 'SB', 'active': True},
    {'sType': E_SEC_FOP,    'root': E_RT_ES,    'tc': E_EMPTY,      'xch': E_XCH_CME,   'mul': E_MUL_50,    'cdn': '',      'step': 5,      'days': 'ABCD',     'active': True},
]

PERM = [
    {'root':E_RT_SPX  ,'tc':E_EMPTY         ,'xch': E_XCH_CBOE     ,'conid': b"416904\x00"  , 'sType':E_SEC_IND ,'port':0,'active': True},
    {'root':E_RT_VIX  ,'tc':E_EMPTY         ,'xch': E_XCH_CBOE     ,'conid': b"13455763\x00", 'sType':E_SEC_IND ,'port':0,'active': True},
    {'root':E_RT_SPY  ,'tc':E_EMPTY         ,'xch': E_XCH_ARCA     ,'conid': b"756733\x00"  , 'sType':E_SEC_STK ,'port':0,'active': True},
    {'root':E_RT_EUR  ,'tc':E_TC_EURUSD     ,'xch': E_XCH_IDEALPRO ,'conid': b"12087792\x00", 'sType':E_SEC_CASH,'port':0,'active': True},
    {'root':E_RT_GBP  ,'tc':E_TC_GBPUSD     ,'xch': E_XCH_IDEALPRO ,'conid': b"12087797\x00", 'sType':E_SEC_CASH,'port':0,'active': True},
    {'root':E_RT_USD  ,'tc':E_TC_USDJPY     ,'xch': E_XCH_IDEALPRO ,'conid': b"15016059\x00", 'sType':E_SEC_CASH,'port':0,'active': True},
]

FUT = [
    {'root':E_RT_VIX  ,'tc':E_TC_VX         ,'xch': E_XCH_CFE,   'mul':E_MUL_1000   ,'freq': MONTHLY,    'count' : 4, 'port':1, 'active': True},
    {'root':E_RT_EUR  ,'tc':E_TC_6E         ,'xch': E_XCH_CME,   'mul':E_MUL_125K   ,'freq': QUARTERLY,  'count' : 2, 'port':0, 'active': True},
    {'root':E_RT_GBP  ,'tc':E_TC_6B         ,'xch': E_XCH_CME,   'mul':E_MUL_62K    ,'freq': QUARTERLY,  'count' : 2, 'port':0, 'active': True},
    {'root':E_RT_JPY  ,'tc':E_TC_6J         ,'xch': E_XCH_CME,   'mul':E_MUL_6J     ,'freq': QUARTERLY,  'count' : 2, 'port':0, 'active': True},
    {'root':E_RT_ES   ,'tc':E_EMPTY         ,'xch': E_XCH_CME,   'mul':E_MUL_50     ,'freq': QUARTERLY,  'count' : 2, 'port':0, 'active': True},
    {'root':E_RT_CL   ,'tc':E_EMPTY         ,'xch': E_XCH_NYMEX, 'mul':E_MUL_1000   ,'freq': MONTHLY,    'count' : 4, 'port':0, 'active': True}

]

OPT = [
    {'root':E_RT_SPX  ,'tc':E_TC_SPXW       ,'xch': E_XCH_CBOE,  'mul':E_MUL_100,   'cdn':'_SPX', 'step': 5,'base_expiry_index': 50, 'other_tc':E_TC_SPX ,'active': True},
    {'root':E_RT_SPY  ,'tc':E_EMPTY         ,'xch': E_XCH_CBOE,  'mul':E_EMPTY,     'cdn': 'SPY', 'step': 1,'base_expiry_index': None, 'active': True},
]

FOP = [
    {'root':E_RT_EUR  ,'tc':E_EMPTY         ,'xch': E_XCH_CME,   'mul':E_MUL_125K,  'cdn': ''   , 'step': 0.0025 ,'days':'OUEU', 'active': True},
    {'root':E_RT_VIX  ,'tc':E_EMPTY         ,'xch': E_XCH_CFE,   'mul':E_MUL_1000,  'cdn': ''   , 'step': 0.5    ,'days':'BGGB', 'suffix':'SB', 'active': True},
    {'root':E_RT_JPY  ,'tc':E_EMPTY         ,'xch': E_XCH_CME,   'mul':E_MUL_1000,  'cdn': ''   , 'step': 0.25   ,'days':'JJJJ', 'suffix': 'SB', 'active': True},
    {'root':E_RT_ES   ,'tc':E_EMPTY         ,'xch': E_XCH_CME,   'mul':E_MUL_50,    'cdn': ''   , 'step': 5      ,'days':'ABCD', 'active': True},
]

def get_root_index(value): return ROOTS.index(value)
def get_tc_index(value): return TCLASSES.index(value)
def get_xch_index(value): return EXCHANGES.index(value)
def get_mul_index(value): return MULTIPLIERS.index(value)

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









REQ_CONTRACT_DETAILS = b"9\x00"  # msgId

MSG_CONTRACT_DETAILS = b"10"  # msgId
MSG_CONTRACT_DETAILS_END = b"52"  # msgId

REQ_OPT_PARAMS = b"78\x00"  # msgId

MSG_OPT_PARAMS = b"75"  # msgId
MSG_OPT_PARAMS_END = b"76"  # msgId

VERSION_8 = b"8\x00"  # version

INCLUDE_EXPIRED_FALSE = b"0\x00"
INCLUDE_EXPIRED_TRUE = b"1\x00"


class CtsChunks:
    # Message header chunks






    # Option rights
    E_CALL = b"C\x00"
    E_PUT = b"P\x00"
    CALL = b"C"
    PUT = b"P"

    # Root chunks
    E_RT_SPX = b"SPX\x00"
    E_RT_VIX = b"VIX\x00"
    E_RT_SPY = b"SPY\x00"
    E_RT_EUR = b"EUR\x00"
    E_RT_GBP = b"GBP\x00"
    E_RT_JPY = b"JPY\x00"
    E_RT_USD = b"USD\x00"
    E_RT_ES = b"ES\x00"
    E_RT_NQ = b"NQ\x00"
    E_RT_CL = b"CL\x00"
    E_RT_GC = b"GC\x00"

    # Root chunks
    RT_SPX = b"SPX"
    RT_VIX = b"VIX"
    RT_SPY = b"SPY"
    RT_EUR = b"EUR"
    RT_GBP = b"GBP"
    RT_JPY = b"JPY"
    RT_USD = b"USD"
    RT_ES = b"ES"
    RT_NQ = b"NQ"
    RT_CL = b"CL"
    RT_GC = b"GC"

    # TClass chunks
    TC_SPX = b"SPX"
    TC_SPXW = b"SPXW"
    TC_VX = b"VX"
    TC_6E = b"6E"
    TC_6B = b"6B"
    TC_6A = b"6A"
    TC_6C = b"6C"
    TC_6J = b"6J"
    TC_6N = b"6N"
    TC_6S = b"6S"
    TC_EURUSD = b"EUR.USD"
    TC_GBPUSD = b"GBP.USD"
    TC_AUDUSD = b"AUD.USD"
    TC_USDCAD = b"USD.CAD"
    TC_USDJPY = b"USD.JPY"
    TC_NZDUSD = b"NZD.USD"
    TC_USDCHF = b"USD.CHF"

    # TClass chunks
    E_TC_SPX = b"SPX\x00"
    E_TC_SPXW = b"SPXW\x00"
    E_TC_VX = b"VX\x00"
    E_TC_6E = b"6E\x00"
    E_TC_6B = b"6B\x00"
    E_TC_6A = b"6A\x00"
    E_TC_6C = b"6C\x00"
    E_TC_6J = b"6J\x00"
    E_TC_6N = b"6N\x00"
    E_TC_6S = b"6S\x00"
    E_TC_EURUSD = b"EUR.USD\x00"
    E_TC_GBPUSD = b"GBP.USD\x00"
    E_TC_AUDUSD = b"AUD.USD\x00"
    E_TC_USDCAD = b"USD.CAD\x00"
    E_TC_USDJPY = b"USD.JPY\x00"
    E_TC_NZDUSD = b"NZD,USD\x00"
    E_TC_USDCHF = b"USD.CHF\x00"

    # Currency chunks
    E_CUR_AUD = b"AUD\x00"
    E_CUR_CAD = b"CAD\x00"
    E_CUR_GBP = b"GBP\x00"
    E_CUR_EUR = b"EUR\x00"
    E_CUR_USD = b"USD\x00"
    E_CUR_JPY = b"JPY\x00"

    # Currency chunks
    CUR_AUD = b"AUD"
    CUR_CAD = b"CAD"
    CUR_GBP = b"GBP"
    CUR_EUR = b"EUR"
    CUR_USD = b"USD"
    CUR_JPY = b"JPY"

    # month chunks
    E_MTH_F = b"01\x00"
    E_MTH_G = b"02\x00"
    E_MTH_H = b"03\x00"
    E_MTH_J = b"04\x00"
    E_MTH_K = b"05\x00"
    E_MTH_M = b"06\x00"
    E_MTH_N = b"07\x00"
    E_MTH_Q = b"08\x00"
    E_MTH_U = b"09\x00"
    E_MTH_V = b"10\x00"
    E_MTH_X = b"11\x00"
    E_MTH_Z = b"12\x00"

    # month chunks
    MTH_F = b"F"
    MTH_G = b"G"
    MTH_H = b"H"
    MTH_J = b"J"
    MTH_K = b"K"
    MTH_M = b"M"
    MTH_N = b"N"
    MTH_Q = b"Q"
    MTH_U = b"U"
    MTH_V = b"V"
    MTH_X = b"X"
    MTH_Z = b"Z"

    # year chunks
    YR_25 = b"2025"
    YR_26 = b"2026"
    YR_27 = b"2027"
    YR_28 = b"2028"
    YR_29 = b"2029"
    YR_30 = b"2030"

    # year chunks
    E_YR_25 = b"5\x00"
    E_YR_26 = b"6\x00"
    E_YR_27 = b"7\x00"
    E_YR_28 = b"8\x00"
    E_YR_29 = b"9\x00"
    E_YR_30 = b"0\x00"

    # Security type chunks
    E_SEC_STK = b"STK\x00"
    E_SEC_CASH = b"CASH\x00"
    E_SEC_FUT = b"FUT\x00"
    E_SEC_IND = b"IND\x00"
    E_SEC_OPT = b"OPT\x00"
    E_SEC_FOP = b"FOP\x00"

    # Security type chunks
    SEC_STK = b"STK"
    SEC_CASH = b"CASH"
    SEC_FUT = b"FUT"
    SEC_IND = b"IND"
    SEC_OPT = b"OPT"
    SEC_FOP = b"FOP"


    # Security type chunks
    E_MUL_50 = b"50\x00"
    E_MUL_100 = b"100\x00"
    E_MUL_1000 = b"1000\x00"
    E_MUL_100K = b"100000\x00"
    E_MUL_125K = b"125000\x00"
    E_MUL_62K = b"62500\x00"
    E_MUL_6J = b"12500000\x00"

    # Security type chunks
    MUL_50 = b"50"
    MUL_100 = b"100"
    MUL_1000 = b"1000"
    MUL_100K = b"100000"
    MUL_125K = b"125000"
    MUL_62K = b"62500"
    MUL_6J = b"12500000"

    # Exchange chunks (common ones)
    E_XCH_CFE = b"CFE\x00"
    E_XCH_ARCA = b"ARCA\x00"
    E_XCH_NASDAQ = b"NASDAQ\x00"
    E_XCH_NYMEX = b"NYMEX\x00"
    E_XCH_CME = b"CME\x00"
    E_XCH_SMART = b"SMART\x00"
    E_XCH_CBOE = b"CBOE\x00"
    E_XCH_IDEALPRO = b"IDEALPRO\x00"

    # Exchange chunks (common ones)
    XCH_CFE = b"CFE"
    XCH_ARCA = b"ARCA"
    XCH_NASDAQ = b"NASDAQ"
    XCH_NYMEX = b"NYMEX"
    XCH_CME = b"CME"
    XCH_SMART = b"SMART"
    XCH_CBOE = b"CBOE"
    XCH_IDEALPRO = b"IDEALPRO"

    RT_BAR_SIZE = b"5\x00"

