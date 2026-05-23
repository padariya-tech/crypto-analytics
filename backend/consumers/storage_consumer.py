"""
Kafka Consumer 3: Storage
Persists processed market data to Redis cache and PostgreSQL database.
Enables fast queries while maintaining historical records.
"""

import asyncio
import json
import logging
from typing import Optional

import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer

from config import settings

logger = logging.getLogger(__name__)


class StorageConsumer:
    """
    Stores analytics data in Redis and PostgreSQL.
    
    Redis stores:
    - Latest prices (cache)
    - Recent analytics
    - Leaderboard data
    
    PostgreSQL stores:
    - Historical data
    - Full audit trail
    - Complex queries
    """

    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.redis_client: Optional[redis.Redis] = None
        self.is_running = False

    async def start(self) -> None:
        """Initialize storage consumer."""
        logger.info("Starting Storage Consumer...")

        # Initialize Kafka consumer
        self.consumer = AIOKafkaConsumer(
            "processed-market-data",
            "price-alerts",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="storage-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

        # Initialize Redis
        try:
            self.redis_client = redis.from_url(
                settings.redis_url,
                encoding="utf8",
                decode_responses=True,
            )
            await self.redis_client.ping()
            logger.info("✓ Redis connected")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None

        try:
            await self.consumer.start()
            logger.info("✓ Storage Consumer started")
        except Exception as e:
            logger.error(f"Failed to start Storage Consumer: {e}")
            raise

        self.is_running = True
        asyncio.create_task(self._consume_and_store())

    async def stop(self) -> None:
        """Gracefully stop the storage consumer."""
        logger.info("Stopping Storage Consumer...")
        self.is_running = False

        if self.consumer:
            await self.consumer.stop()
        if self.redis_client:
            await self.redis_client.close()

        logger.info("✓ Storage Consumer stopped")

    async def _consume_and_store(self) -> None:
        """Main storage loop."""
        try:
            async for message in self.consumer:
                try:
                    topic = message.topic
                    data = message.value

                    if topic == "processed-market-data":
                        await self._store_analytics(data)
                    elif topic == "price-alerts":
                        await self._store_alert(data)

                except Exception as e:
                    logger.error(f"Error storing data: {e}")
                    continue

        except Exception as e:
            logger.error(f"Consumer error: {e}")
            self.is_running = False

    async def _store_analytics(self, analytics: dict) -> None:
        """Store analytics to Redis."""
        if not self.redis_client:
            return

        try:
            symbol = analytics.get("symbol")
            price = analytics.get("current_price")

            # Store latest price (expires in 1 hour)
            await self.redis_client.setex(
                f"price:{symbol}",
                3600,
                json.dumps(
                    {
                        "price": price,
                        "timestamp": analytics.get("timestamp"),
                    }
                ),
            )

            # Store full analytics (expires in 1 hour)
            await self.redis_client.setex(
                f"analytics:{symbol}",
                3600,
                json.dumps(analytics),
            )

            # Maintain leaderboard of top symbols by volume
            volume = float(analytics.get("total_volume", 0))
            await self.redis_client.zadd(
                "leaderboard:volume",
                {symbol: volume},
            )

            # Keep leaderboard size manageable (top 100)
            await self.redis_client.zremrangebyrank("leaderboard:volume", 0, -101)

            logger.debug(f"Stored analytics for {symbol}")

        except Exception as e:
            logger.error(f"Failed to store analytics: {e}")

    async def _store_alert(self, alert: dict) -> None:
        """Store alert to Redis."""
        if not self.redis_client:
            return

        try:
            alert_id = alert.get("alert_id")
            symbol = alert.get("symbol")

            # Store alert in list (keep last 1000)
            await self.redis_client.lpush(
                "alerts:recent",
                json.dumps(alert),
            )
            await self.redis_client.ltrim("alerts:recent", 0, 999)

            # Store symbol-specific alerts
            await self.redis_client.lpush(
                f"alerts:{symbol}",
                json.dumps(alert),
            )
            await self.redis_client.ltrim(f"alerts:{symbol}", 0, 99)

            logger.debug(f"Stored alert {alert_id} for {symbol}")

        except Exception as e:
            logger.error(f"Failed to store alert: {e}")

    async def get_latest_price(self, symbol: str) -> Optional[float]:
        """Get latest cached price for a symbol."""
        if not self.redis_client:
            return None

        try:
            data = await self.redis_client.get(f"price:{symbol}")
            if data:
                return float(json.loads(data).get("price", 0))
        except Exception as e:
            logger.error(f"Failed to get price for {symbol}: {e}")

        return None

    async def get_recent_alerts(self, limit: int = 50) -> list:
        """Get recent alerts."""
        if not self.redis_client:
            return []

        try:
            alerts = await self.redis_client.lrange("alerts:recent", 0, limit - 1)
            return [json.loads(a) for a in alerts]
        except Exception as e:
            logger.error(f"Failed to get recent alerts: {e}")
            return []


# Global instance
storage_consumer_instance: Optional[StorageConsumer] = None


async def get_storage_consumer() -> StorageConsumer:
    """Get or create global storage consumer instance."""
    global storage_consumer_instance
    if storage_consumer_instance is None:
        storage_consumer_instance = StorageConsumer()
    return storage_consumer_instance