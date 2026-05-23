"""
Kafka Consumer 1: Analytics
Processes trades in real-time to calculate:
- Moving averages
- Price changes
- Trade counts and volumes
- Volume spikes and anomalies
"""

import asyncio
import json
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config import settings

logger = logging.getLogger(__name__)


class AnalyticsConsumer:
    """
    Processes crypto trades to generate analytics.
    
    Maintains sliding windows of trade data per symbol:
    - Moving average (last 20 trades)
    - Volume metrics
    - Price change tracking
    - Anomaly detection
    """

    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_running = False

        # State tracking (per symbol)
        self.price_windows: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=settings.analytics_window_size)
        )
        self.volume_windows: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=60)  # Last 60 trades (~1 minute)
        )
        self.last_price: Dict[str, float] = {}
        self.last_alert_time: Dict[str, datetime] = defaultdict(lambda: datetime.now())

    async def start(self) -> None:
        """Initialize and start the analytics consumer."""
        logger.info("Starting Analytics Consumer...")

        # Create consumer
        self.consumer = AIOKafkaConsumer(
            "crypto-trades",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="analytics-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

        # Create producer for publishing analytics
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )

        try:
            await self.consumer.start()
            await self.producer.start()
            logger.info("✓ Analytics Consumer started")
        except Exception as e:
            logger.error(f"Failed to start Analytics Consumer: {e}")
            raise

        self.is_running = True
        asyncio.create_task(self._consume_and_analyze())

    async def stop(self) -> None:
        """Gracefully stop the consumer."""
        logger.info("Stopping Analytics Consumer...")
        self.is_running = False

        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

        logger.info("✓ Analytics Consumer stopped")

    async def _consume_and_analyze(self) -> None:
        """Main consumer loop."""
        try:
            async for message in self.consumer:
                try:
                    trade = message.value
                    symbol = trade.get("symbol")

                    # Update windows
                    price = float(trade.get("price", 0))
                    quantity = float(trade.get("quantity", 0))

                    self.price_windows[symbol].append(price)
                    self.volume_windows[symbol].append(quantity)

                    # Calculate analytics
                    analytics = self._calculate_analytics(symbol, trade)

                    # Publish analytics
                    if analytics:
                        await self._publish_analytics(analytics)

                    # Update last price
                    self.last_price[symbol] = price

                except Exception as e:
                    logger.error(f"Error analyzing trade: {e}")
                    continue

        except Exception as e:
            logger.error(f"Consumer error: {e}")
            self.is_running = False

    def _calculate_analytics(self, symbol: str, trade: dict) -> Optional[dict]:
        """
        Calculate analytics metrics for a symbol.
        
        Returns:
            Analytics dictionary or None if insufficient data.
        """
        prices = self.price_windows[symbol]
        volumes = self.volume_windows[symbol]

        if not prices or not volumes:
            return None

        current_price = float(trade.get("price", 0))
        previous_price = self.last_price.get(symbol, current_price)

        # Calculate moving average
        moving_avg = sum(prices) / len(prices)

        # Price change
        price_change_pct = (
            ((current_price - previous_price) / previous_price * 100)
            if previous_price > 0
            else 0
        )

        # Volume metrics
        total_volume = sum(volumes)
        avg_volume = total_volume / len(volumes) if volumes else 0
        volume_spike_ratio = (
            (float(trade.get("quantity", 0)) / avg_volume)
            if avg_volume > 0
            else 1.0
        )

        # Price range
        high_price = max(prices)
        low_price = min(prices)

        # Detect anomalies
        is_volume_spike = volume_spike_ratio > settings.volume_spike_threshold
        is_rapid_price_movement = abs(price_change_pct) > settings.rapid_price_movement_threshold

        analytics = {
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
            "current_price": current_price,
            "moving_average_20": moving_avg,
            "price_change_percent": price_change_pct,
            "trades_per_minute": len(volumes),
            "total_volume": total_volume,
            "high_price": high_price,
            "low_price": low_price,
            "volume_spike_ratio": volume_spike_ratio,
            "is_volume_spike": is_volume_spike,
            "is_rapid_movement": is_rapid_price_movement,
        }

        logger.debug(
            f"{symbol}: ${current_price} | MA20: ${moving_avg:.2f} | "
            f"Change: {price_change_pct:+.2f}% | Vol/Min: {len(volumes)}"
        )

        return analytics

    async def _publish_analytics(self, analytics: dict) -> None:
        """Publish analytics to processed-market-data topic."""
        try:
            symbol = analytics.get("symbol")
            await self.producer.send_and_wait(
                "processed-market-data",
                value=analytics,
                key=symbol.encode("utf-8"),
            )
        except Exception as e:
            logger.error(f"Failed to publish analytics: {e}")


# Global instance
analytics_consumer_instance: Optional[AnalyticsConsumer] = None


async def get_analytics_consumer() -> AnalyticsConsumer:
    """Get or create global analytics consumer instance."""
    global analytics_consumer_instance
    if analytics_consumer_instance is None:
        analytics_consumer_instance = AnalyticsConsumer()
    return analytics_consumer_instance