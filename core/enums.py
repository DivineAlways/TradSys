"""All enumerations used across the trading system."""
from enum import Enum, auto


class OrderStatus(str, Enum):
    """
    Order state machine:

        NEW → SUBMITTED → ACCEPTED → PARTIAL_FILL → FILLED
                                   ↘ CANCELLED
                                   ↘ REJECTED
                                   ↘ EXPIRED
    """
    NEW = "new"                    # created locally, not yet sent to broker
    SUBMITTED = "submitted"        # sent to broker, awaiting ACK
    ACCEPTED = "accepted"          # broker acknowledged
    PARTIAL_FILL = "partial_fill"  # partially filled
    FILLED = "filled"              # fully filled
    CANCELLED = "cancelled"        # cancelled by us or broker
    REJECTED = "rejected"          # rejected by broker
    EXPIRED = "expired"            # time-in-force expired
    PENDING_CANCEL = "pending_cancel"  # cancel request sent


# Transitions allowed by the state machine
ORDER_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.NEW: {OrderStatus.SUBMITTED, OrderStatus.CANCELLED},
    OrderStatus.SUBMITTED: {
        OrderStatus.ACCEPTED, OrderStatus.REJECTED,
        OrderStatus.CANCELLED, OrderStatus.FILLED,
    },
    OrderStatus.ACCEPTED: {
        OrderStatus.PARTIAL_FILL, OrderStatus.FILLED,
        OrderStatus.PENDING_CANCEL, OrderStatus.CANCELLED, OrderStatus.EXPIRED,
    },
    OrderStatus.PARTIAL_FILL: {
        OrderStatus.FILLED, OrderStatus.PENDING_CANCEL,
        OrderStatus.CANCELLED, OrderStatus.EXPIRED,
    },
    OrderStatus.PENDING_CANCEL: {OrderStatus.CANCELLED, OrderStatus.FILLED},
    # Terminal states – no transitions out
    OrderStatus.FILLED: set(),
    OrderStatus.CANCELLED: set(),
    OrderStatus.REJECTED: set(),
    OrderStatus.EXPIRED: set(),
}

TERMINAL_STATUSES = {
    OrderStatus.FILLED, OrderStatus.CANCELLED,
    OrderStatus.REJECTED, OrderStatus.EXPIRED,
}


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class TimeInForce(str, Enum):
    DAY = "day"
    GTC = "gtc"      # Good Till Cancelled
    IOC = "ioc"      # Immediate Or Cancel
    FOK = "fok"      # Fill Or Kill
    OPG = "opg"      # At Open
    CLS = "cls"      # At Close


class SignalDirection(str, Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"     # close / exit signal


class AssetClass(str, Enum):
    US_EQUITY = "us_equity"
    CRYPTO = "crypto"
    OPTION = "option"


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
