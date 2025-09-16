# cts_java_dll.py - Java-style contract service implementation
import struct
from io import BytesIO
from cts.cts_cfg import root, exch, CUR


class JavaBuilder:
    """Python implementation of Java Builder class"""

    def __init__(self):
        self.buffer = BytesIO()

    def send(self, value) -> None:
        """Send any value with null terminator"""
        if value is None:
            value = ""

        str_value = str(value) if not isinstance(value, str) else value

        # Validate ASCII printable
        if not self._is_ascii_printable(str_value):
            raise ValueError(f"Invalid symbol: {str_value}")

        # Write string bytes + null terminator
        self.buffer.write(str_value.encode('utf-8'))
        self.buffer.write(b'\x00')

    def send_max(self, value) -> None:
        """Send value or empty string if MAX_VALUE"""
        if isinstance(value, int) and value == 2147483647:  # Integer.MAX_VALUE
            self.send("")
        elif isinstance(value, float) and value == float('inf'):  # Double.MAX_VALUE
            self.send("")
        else:
            self.send(value)

    def send_contract(self, contract) -> None:
        """Send contract fields in Java order"""
        self.send(contract.conid)
        self.send(contract.symbol)
        self.send(contract.sec_type)
        self.send(contract.last_trade_date_or_contract_month)
        self.send_max(contract.strike)
        self.send(contract.right)
        self.send(contract.multiplier)
        self.send(contract.exchange)
        self.send(contract.primary_exch)
        self.send(contract.currency)
        self.send(contract.local_symbol)
        self.send(contract.trading_class)
        self.send(1 if contract.include_expired else 0)

    @staticmethod
    def _is_ascii_printable(text: str) -> bool:
        """Check if string contains only ASCII printable characters"""
        if not text:
            return True
        return all(32 <= ord(c) < 127 or c in '\t\n\r' for c in text)

    def get_bytes(self) -> bytes:
        """Get complete buffer as bytes"""
        return self.buffer.getvalue()


class JavaEMessage:
    """Python implementation of Java EMessage class"""

    def __init__(self, builder: JavaBuilder):
        self.data = builder.get_bytes()

    def get_raw_data(self) -> bytes:
        """Get raw message data"""
        return self.data

    def get_size(self) -> int:
        """Get message size"""
        return len(self.data)


class JavaContract:
    """Python implementation of Java Contract class"""

    def __init__(self):
        self.conid = 0
        self.symbol = ""
        self.sec_type = ""
        self.last_trade_date_or_contract_month = ""
        self.last_trade_date = ""
        self.strike = float('inf')  # Double.MAX_VALUE
        self.right = ""
        self.multiplier = ""
        self.exchange = ""
        self.primary_exch = ""
        self.currency = ""
        self.local_symbol = ""
        self.trading_class = ""
        self.sec_id_type = ""
        self.sec_id = ""
        self.description = ""
        self.issuer_id = ""
        self.include_expired = False


class JavaUtil:
    """Python implementation of Java Util class"""

    @staticmethod
    def string_is_empty(value: str) -> bool:
        """Check if string is null or empty"""
        return value is None or len(value) == 0

    @staticmethod
    def double_max_string(value: float) -> str:
        """Convert double to string, empty if MAX_VALUE"""
        return "" if value == float('inf') else str(value)

    @staticmethod
    def int_max_string(value: int) -> str:
        """Convert int to string, empty if MAX_VALUE"""
        return "" if value == 2147483647 else str(value)


class JavaCtsClient:
    """Java-style contract client implementation"""

    def __init__(self, prt):
        self.prt = prt
        self.server_version = prt.server_version

    def _prepare_buffer(self) -> JavaBuilder:
        """Prepare message builder"""
        return JavaBuilder()

    def _send_msg_id(self, builder: JavaBuilder, msg_id: int) -> None:
        """Send message ID - raw int for server 201+, string otherwise"""
        if self.server_version >= 201:
            # Send as raw 4-byte big-endian integer
            int_bytes = struct.pack(">I", msg_id)
            builder.buffer.write(int_bytes)
        else:
            builder.send(msg_id)

    def _close_and_send(self, builder: JavaBuilder) -> None:
        """Frame and send message"""
        message = JavaEMessage(builder)
        raw_data = message.get_raw_data()

        # Frame with 4-byte length prefix
        frame_length = struct.pack(">I", len(raw_data))
        framed_message = frame_length + raw_data

        print(f">>> Java-style: {len(framed_message)} bytes: {framed_message[:32].hex()}...")
        self.prt.send(framed_message)

    def _is_empty(self, value: str) -> bool:
        """Check if string is empty (Java IsEmpty equivalent)"""
        return JavaUtil.string_is_empty(value)

    def req_contract_details(self, req_id: int, contract: JavaContract) -> None:
        """Request contract details - exact Java implementation"""
        if not self.prt.is_connected():
            raise ConnectionError("Not connected")

        # Version checks from Java code
        if self.server_version < 4:
            raise ValueError("Server does not support contract details")

        if (self.server_version >= 45 or
                (self._is_empty(contract.sec_id_type) and self._is_empty(contract.sec_id))):

            # Additional version checks
            if self.server_version < 68 and not self._is_empty(contract.trading_class):
                raise ValueError("Server does not support tradingClass parameter")

            if self.server_version < 70 and not self._is_empty(contract.primary_exch):
                raise ValueError("Server does not support primaryExchange parameter")

            if self.server_version < 176 and not self._is_empty(contract.issuer_id):
                raise ValueError("Server does not support issuerId parameter")

            try:
                builder = self._prepare_buffer()
                self._send_msg_id(builder, 9)  # Contract details message ID
                builder.send(8)  # Version

                if self.server_version >= 40:
                    builder.send(req_id)

                if self.server_version >= 37:
                    builder.send(contract.conid)

                builder.send(contract.symbol)
                builder.send(contract.sec_type)
                builder.send(contract.last_trade_date_or_contract_month)
                builder.send_max(contract.strike)
                builder.send(contract.right)

                if self.server_version >= 15:
                    builder.send(contract.multiplier)

                if self.server_version >= 75:
                    builder.send(contract.exchange)
                    builder.send(contract.primary_exch)
                elif self.server_version >= 70:
                    if (self._is_empty(contract.primary_exch) or
                            (contract.exchange != "BEST" and contract.exchange != "SMART")):
                        builder.send(contract.exchange)
                    else:
                        builder.send(f"{contract.exchange}:{contract.primary_exch}")

                builder.send(contract.currency)
                builder.send(contract.local_symbol)

                if self.server_version >= 68:
                    builder.send(contract.trading_class)

                if self.server_version >= 31:
                    builder.send(contract.include_expired)

                if self.server_version >= 45:
                    builder.send(contract.sec_id_type)
                    builder.send(contract.sec_id)

                if self.server_version >= 176:
                    builder.send(contract.issuer_id)

                self._close_and_send(builder)

            except Exception as e:
                raise RuntimeError(f"Failed to send contract details request: {e}")
        else:
            raise ValueError("Server does not support secIdType and secId parameters")

    def req_sec_def_opt_params(self, req_id: int, underlying_symbol: str,
                               fut_fop_exchange: str, underlying_sec_type: str,
                               underlying_con_id: int) -> None:
        """Request security definition option parameters"""
        if not self.prt.is_connected():
            raise ConnectionError("Not connected")

        if self.server_version < 104:
            raise ValueError("Server does not support security definition option requests")

        try:
            builder = self._prepare_buffer()
            self._send_msg_id(builder, 78)  # SecDefOptParams message ID
            builder.send(req_id)
            builder.send(underlying_symbol)
            builder.send(fut_fop_exchange)
            builder.send(underlying_sec_type)
            builder.send(underlying_con_id)

            self._close_and_send(builder)

        except Exception as e:
            raise RuntimeError(f"Failed to send sec def opt params request: {e}")


def create_spx_contract() -> JavaContract:
    """Create SPX index contract using indexed configuration"""
    contract = JavaContract()

    # Use indexed configuration
    spx_idx = 0  # SPX is index 0 in root
    cboe_idx = 0  # CBOE is index 0 in exch
    usd_idx = 0  # USD is index 0 in CUR

    contract.symbol = root[spx_idx][0]  # "SPX"
    contract.sec_type = "IND"
    contract.exchange = exch[cboe_idx]  # "CBOE"
    contract.currency = CUR[usd_idx]  # "USD"
    contract.primary_exch = ""
    contract.local_symbol = ""
    contract.trading_class = ""
    contract.include_expired = False
    contract.sec_id_type = ""
    contract.sec_id = ""
    contract.issuer_id = ""

    return contract


# Test function
async def test_java_style_request(prt):
    """Test Java-style contract request"""
    print("=== Java-Style Contract Request ===")

    try:
        client = JavaCtsClient(prt)
        contract = create_spx_contract()

        print(f"Contract: {contract.symbol} {contract.sec_type} on {contract.exchange}")
        print(f"Server version: {client.server_version}")

        # Send request
        client.req_contract_details(1001, contract)

        # Monitor for responses manually
        print("Request sent. Monitor responses manually...")
        return True

    except Exception as e:
        print(f"Java-style request failed: {e}")
        return False


# Example usage for comparison
def compare_requests():
    """Compare byte output between implementations"""
    print("=== Request Format Comparison ===")

    # Show what the Java-style builder creates
    builder = JavaBuilder()
    builder.send(9)  # msgId
    builder.send(8)  # version
    builder.send(1001)  # reqId
    builder.send(0)  # conId
    builder.send("SPX")  # symbol
    builder.send("IND")  # secType
    builder.send("")  # lastTradeDateOrContractMonth
    builder.send_max(float('inf'))  # strike (MAX_VALUE)
    builder.send("")  # right
    builder.send("")  # multiplier
    builder.send("CBOE")  # exchange
    builder.send("")  # primaryExch
    builder.send("USD")  # currency
    builder.send("")  # localSymbol
    builder.send("")  # tradingClass
    builder.send(0)  # includeExpired
    builder.send("")  # secIdType
    builder.send("")  # secId
    builder.send("")  # issuerId

    payload = builder.get_bytes()
    frame_length = struct.pack(">I", len(payload))
    full_message = frame_length + payload

    print(f"Java-style payload: {payload.hex()}")
    print(f"Java-style framed: {full_message.hex()}")

    return full_message


__all__ = [
    "JavaCtsClient",
    "JavaContract",
    "create_spx_contract",
    "test_java_style_request",
    "compare_requests"
]