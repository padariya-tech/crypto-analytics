"""
Message schemas for Kafka topics.
Defines validation for all message types flowing through the system.
"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class TradeMessageSchema(BaseModel):
    """Raw trade message from Binance."""

    symbol: str = Field(..., description="Crypto symbol (e.g., BTCUSDT)")
    price: float = Field(..., gt=0, description="Trade price")
    quantity: float = Field(..., gt=0, description="Trade quantity")
    side: str = Field(..., pattern="^(BUY|SELL)$", description="Trade side")
    timestamp: int = Field(..., description="Trade timestamp in milliseconds")
    event_time: datetime = Field(..., description="Event datetime")
    trade_id: int = Field(..., description="Unique trade ID")


class AnalyticsMessageSchema(BaseModel):
    """Processed analytics from Consumer 1."""

    symbol: str
    timestamp: datetime
    current_price: float
    moving_average_20: Optional[float] = None
    price_change_percent: float
    trades_per_minute: int
    total_volume: float
    high_price: float
    low_price: float


class AlertMessageSchema(BaseModel):
    """Alert from Consumer 2."""

    alert_id: str = Field(..., description="Unique alert ID")
    symbol: str
    alert_type: str = Field(..., pattern="^(PRICE_CHANGE|VOLUME_SPIKE|RAPID_MOVEMENT)$")
    severity: str = Field(..., pattern="^(LOW|MEDIUM|HIGH)$")
    message: str
    current_price: float
    change_value: float
    timestamp: datetime


class AnomalyMessageSchema(BaseModel):
    """Anomaly or error for Dead Letter Queue."""

    original_topic: str
    original_message: str
    error_reason: str
    timestamp: datetime
    retry_count: int


# Type mapping for topic types
TOPIC_SCHEMAS = {
    "crypto-trades": TradeMessageSchema,
    "processed-market-data": AnalyticsMessageSchema,
    "price-alerts": AlertMessageSchema,
    "anomalies": AnomalyMessageSchema,
}
