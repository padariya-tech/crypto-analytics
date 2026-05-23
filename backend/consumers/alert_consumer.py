"""
Kafka Consumer 2: Alert Engine
Monitors price movements and volume spikes to generate alerts.
Publishes alerts to frontend via Kafka and WebSocket.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config import settings

logger = logging.getLogger(__name__)


class AlertEngine:
    """
    Generates real-time alerts based on:
    - Price change thresholds (percentage movement)
    - Volume spike detection
    - Rapid price movements
    
    Implements alert cooldown to prevent flooding.
    """

    def __init__(self):
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
        self.is_running = False

        # Track last alert time per symbol to prevent spam
        self.last_alert_times: Dict[str, Dict[str, datetime]] = {}
        self.alert_cooldown = settings.alert_cooldown

    async def start(self) -> None:
        """Initialize and start the alert consumer."""
        logger.info("Starting Alert Engine...")

        self.consumer = AIOKafkaConsumer(
            "processed-market-data",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="alerts-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )

        try:
            await self.consumer.start()
            await self.producer.start()
            logger.info("✓ Alert Engine started")
        except Exception as e:
            logger.error(f"Failed to start Alert Engine: {e}")
            raise

        self.is_running = True
        asyncio.create_task(self._monitor_and_alert())

    async def stop(self) -> None:
        """Gracefully stop the alert engine."""
        logger.info("Stopping Alert Engine...")
        self.is_running = False

        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

        logger.info("✓ Alert Engine stopped")

    async def _monitor_and_alert(self) -> None:
        """Main monitoring loop."""
        try:
            async for message in self.consumer:
                try:
                    analytics = message.value
                    alerts = self._check_alert_conditions(analytics)

                    for alert in alerts:
                        await self._publish_alert(alert)

                except Exception as e:
                    logger.error(f"Error checking alerts: {e}")
                    continue

        except Exception as e:
            logger.error(f"Consumer error: {e}")
            self.is_running = False

    def _check_alert_conditions(self, analytics: dict) -> List[dict]:
        """
        Check if analytics data triggers any alerts.
        
        Alert types:
        1. Price change alert: Price moved > threshold%
        2. Volume spike: Volume significantly above average
        3. Rapid movement: Price moved > rapid threshold%
        """
        alerts = []
        symbol = analytics.get("symbol")
        price = float(analytics.get("current_price", 0))
        price_change = float(analytics.get("price_change_percent", 0))
        volume_spike_ratio = float(analytics.get("volume_spike_ratio", 1.0))
        is_rapid = analytics.get("is_rapid_movement", False)

        # Price Change Alert
        if abs(price_change) > settings.price_change_threshold:
            if self._check_cooldown(symbol, "price_change"):
                alerts.append(
                    {
                        "type": "PRICE_CHANGE",
                        "symbol": symbol,
                        "severity": "HIGH" if abs(price_change) > 5 else "MEDIUM",
                        "message": f"{symbol} price moved {price_change:+.2f}% to ${price:.2f}",
                        "current_price": price,
                        "change_value": price_change,
                    }
                )
                self._record_alert(symbol, "price_change")

        # Volume Spike Alert
        if volume_spike_ratio > settings.volume_spike_threshold:
            if self._check_cooldown(symbol, "volume_spike"):
                alerts.append(
                    {
                        "type": "VOLUME_SPIKE",
                        "symbol": symbol,
                        "severity": "MEDIUM",
                        "message": f"{symbol} volume spike detected ({volume_spike_ratio:.1f}x average)",
                        "current_price": price,
                        "change_value": volume_spike_ratio,
                    }
                )
                self._record_alert(symbol, "volume_spike")

        # Rapid Movement Alert
        if is_rapid and self._check_cooldown(symbol, "rapid_movement"):
            alerts.append(
                {
                    "type": "RAPID_MOVEMENT",
                    "symbol": symbol,
                    "severity": "HIGH",
                    "message": f"{symbol} rapid price movement detected ({price_change:+.2f}%)",
                    "current_price": price,
                    "change_value": price_change,
                }
            )
            self._record_alert(symbol, "rapid_movement")

        return alerts

    def _check_cooldown(self, symbol: str, alert_type: str) -> bool:
        """
        Check if enough time has passed since last alert of this type.
        Prevents duplicate alerts for the same condition.
        """
        if symbol not in self.last_alert_times:
            self.last_alert_times[symbol] = {}

        last_time = self.last_alert_times[symbol].get(alert_type)
        if last_time is None:
            return True

        time_since_last = (datetime.now() - last_time).total_seconds()
        return time_since_last > self.alert_cooldown

    def _record_alert(self, symbol: str, alert_type: str) -> None:
        """Record the time an alert was generated."""
        if symbol not in self.last_alert_times:
            self.last_alert_times[symbol] = {}
        self.last_alert_times[symbol][alert_type] = datetime.now()

    async def _publish_alert(self, alert: dict) -> None:
        """Publish alert to price-alerts topic."""
        try:
            alert_id = str(uuid.uuid4())
            alert["alert_id"] = alert_id
            alert["timestamp"] = datetime.now().isoformat()

            symbol = alert.get("symbol")
            severity = alert.get("severity", "LOW")

            await self.producer.send_and_wait(
                "price-alerts",
                value=alert,
                key=symbol.encode("utf-8"),
            )

            logger.warning(
                f"[{severity}] {alert['message']} (Alert ID: {alert_id})"
            )

        except Exception as e:
            logger.error(f"Failed to publish alert: {e}")


# Global instance
alert_engine_instance: Optional[AlertEngine] = None


async def get_alert_engine() -> AlertEngine:
    """Get or create global alert engine instance."""
    global alert_engine_instance
    if alert_engine_instance is None:
        alert_engine_instance = AlertEngine()
    return alert_engine_instance