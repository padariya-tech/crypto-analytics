"""
Configuration management for the Crypto Analytics System.
Handles Kafka, Redis, PostgreSQL, Binance API settings.
"""

import os
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    # Kafka Configuration
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_group_id: str = "crypto-analytics-group"

    # Redis Configuration
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")

    # PostgreSQL Configuration (Optional)
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://crypto:crypto@postgres:5432/crypto"
    )
    use_database: bool = os.getenv("USE_DATABASE", "false").lower() == "true"

    # Binance Configuration
    binance_stream_url: str = "wss://stream.binance.com:9443/ws"
    symbols: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    # Alert Thresholds
    price_change_threshold: float = 2.0  # % change to trigger alert
    volume_spike_threshold: float = 1.5  # Multiplier for volume spike
    rapid_price_movement_threshold: float = 5.0  # % in short timeframe

    # FastAPI Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_debug: bool = os.getenv("DEBUG", "false").lower() == "true"

    # WebSocket Configuration
    websocket_heartbeat_interval: int = 30  # seconds
    websocket_max_connections: int = 100

    # Processing Configuration
    analytics_window_size: int = 20  # Messages for moving average
    alert_cooldown: int = 60  # Seconds between duplicate alerts
    max_retries: int = 3
    retry_delay: int = 5


# Global settings instance
settings = Settings()
