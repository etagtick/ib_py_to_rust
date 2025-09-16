from core.core_util import encode_field


class MktChunks:
    """Static binary chunks for trade data requests"""

    # Message header chunks
    REQ_MKT_DATA = b"1\x00"  # msgId for reqMktData
    VERSION_11 = b"11\x00"  # version
    REQ_RT_BAR_DATA = b"50\x00"  # msgId for reqMktData
    VERSION_3 = b"3\x00"  # version


    # Trade-specific tick list (ticks 4,5,8,49 plus supporting ticks)
    #TRADE_TICKS = b"100,101,104,106,107,165,221,225,233,236,258,293,294,295\x00"
    TRADE_TICKS = b"233\x00"
    #TRADE_TICKS = b"100,106,233,236\x00"

    RT_BAR_SIZE = b"5\x00"

    # Subscription flags
    SNAPSHOT_FALSE = b"0\x00"
    SNAPSHOT_TRUE = b"1\x00"
    REGULATORY_FALSE = b"0\x00"
    USE_RTH_FALSE = b"0\x00"
    USE_RTH_TRUE = b"1\x00"
    MKT_OPTIONS_EMPTY = b"\x00"
    RT_BAR_OPTIONS_EMPTY = b"\x00"



    # Currency chunks
    TRADES = b"TRADES\x00"
    MIDPOINT = b"MIDPOINT\x00"
    BID = b"BID\x00"
    ASK = b"ASK\x00"
    @staticmethod
    def get_what_to_show_chunk(what_to_show):
        """Get pre-encoded currency chunk"""
        what_to_show_map = {
            'TRADES': MktChunks.TRADES,
            'MIDPOINT': MktChunks.MIDPOINT,
            'BID': MktChunks.BID,
            'ASK': MktChunks.ASK,
        }
        return what_to_show_map.get(what_to_show, encode_field(what_to_show))



