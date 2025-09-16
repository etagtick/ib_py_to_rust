from core.core_util import encode_field

class HstChunks:
    """Static binary chunks for historical data requests"""

    # Message header chunks

    REQ_HST_DATA = b"20\x00"  # msgId for reqHistoricalData
    MSG_HST_DATA = b"17"  # msgId for reqHistoricalData
    MSG_HST_DATA_END = b"18"  # msgId for reqHistoricalData
    VERSION_6 = b"6\x00"  # version (only if server < 124)


    # Historical data specific chunks
    FORMAT_DATE_STANDARD = b"1\x00"  # yyyyMMdd HH:mm:ss format
    KEEP_UP_TO_DATE_FALSE = b"0\x00"
    CHART_OPTIONS_EMPTY = b"\x00"


    # Common bar sizes
    BAR_SIZE_1_MIN = b"1 min\x00"
    BAR_SIZE_5_MIN = b"5 mins\x00"
    BAR_SIZE_1_HOUR = b"1 hour\x00"
    BAR_SIZE_1_DAY = b"1 day\x00"
    @staticmethod
    def get_bar_size_chunk(bar_size):
        """Get pre-encoded bar size chunk"""
        bar_map = {
            '1 min': HstChunks.BAR_SIZE_1_MIN,
            '5 mins': HstChunks.BAR_SIZE_5_MIN,
            '1 hour': HstChunks.BAR_SIZE_1_HOUR,
            '1 day': HstChunks.BAR_SIZE_1_DAY,
        }
        return bar_map.get(bar_size, encode_field(bar_size))

    # Common durations
    DURATION_1_DAY = b"1 D\x00"
    DURATION_2_DAYS = b"2 D\x00"
    DURATION_1_WEEK = b"1 W\x00"
    DURATION_1_MONTH = b"1 M\x00"
    @staticmethod
    def get_duration_chunk(duration):
        """Get pre-encoded duration chunk"""
        duration_map = {
            '1 D': HstChunks.DURATION_1_DAY,
            '2 D': HstChunks.DURATION_2_DAYS,
            '1 W': HstChunks.DURATION_1_WEEK,
            '1 M': HstChunks.DURATION_1_MONTH,
        }
        return duration_map.get(duration, encode_field(duration))