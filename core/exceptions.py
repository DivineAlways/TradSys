"""Domain exception hierarchy."""


class TradSysError(Exception):
    """Base exception for all trading system errors."""


class BrokerError(TradSysError):
    """Errors from broker API communication."""


class BrokerConnectionError(BrokerError):
    """Could not connect to broker."""


class BrokerOrderError(BrokerError):
    """Order placement / management error."""
    def __init__(self, message: str, order_id: str = "", broker_code: str = "") -> None:
        super().__init__(message)
        self.order_id = order_id
        self.broker_code = broker_code


class InvalidOrderTransitionError(TradSysError):
    """Attempted an invalid order state machine transition."""
    def __init__(self, current: str, attempted: str) -> None:
        super().__init__(
            f"Invalid order transition: {current!r} → {attempted!r}"
        )
        self.current = current
        self.attempted = attempted


class RiskLimitBreached(TradSysError):
    """A risk limit was hit; order was blocked."""
    def __init__(self, message: str, limit_name: str = "") -> None:
        super().__init__(message)
        self.limit_name = limit_name


class KillSwitchActive(TradSysError):
    """Kill switch is active; all trading is suspended."""


class ReconciliationError(TradSysError):
    """Unresolvable discrepancy between internal state and broker state."""


class MarketDataError(TradSysError):
    """Market data feed error."""


class ConfigurationError(TradSysError):
    """Missing or invalid configuration."""
