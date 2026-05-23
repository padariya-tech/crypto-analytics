"""
Kafka Producer: Binance WebSocket Stream
Connects to Binance live price feed and publishes validated trades to Kafka.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import aiohttp
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, ValidationError

from config import settings

logger = logging.getLogger(__name__)


class TradeMessage(BaseModel):
    """Validated trade message schema."""

    symbol: str
    price: float
    quantity: float
    side: str  # 'BUY' or 'SELL'
    timestamp: int
    event_time: datetime
    trade_id: int

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "BTCUSDT",
                "price": 45000.50,
                "quantity": 0.5,
                "side": "BUY",
                "timestamp": 1699564800000,
                "event_time": "2024-01-01T12:00:00Z",
                "trade_id": 123456,
            }
        }


class BinanceProducer:
    """
    Kafka Producer for Binance WebSocket data.
    
    Features:
    - Connects to Binance WebSocket stream
    - Validates incoming trade data
    - Handles reconnection logic
    - Publishes to Kafka with error handling
    - Implements exponential backoff for retries
    """

    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.websocket_session: Optional[aiohttp.ClientSession] = None
        self.symbols = settings.symbols
        self.is_running = False
        self.retry_count = 0
        self.max_retries = settings.max_retries

    async def start(self) -> None:
        """Initialize Kafka producer and start streaming."""
        logger.info("Starting Binance Producer...")

        # Initialize Kafka producer
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",  # Wait for all replicas to acknowledge
            )
            await self.producer.start()
            logger.info("✓ Kafka Producer started")
        except Exception as e:
            logger.error(f"Failed to start Kafka Producer: {e}")
            raise

        # Initialize HTTP session for WebSocket
        self.websocket_session = aiohttp.ClientSession()

        # Start streaming
        self.is_running = True
        asyncio.create_task(self._stream_trades())
        logger.info("✓ Trade streaming started")

    async def stop(self) -> None:
        """Gracefully stop the producer."""
        logger.info("Stopping Binance Producer...")
        self.is_running = False

        if self.producer:
            await self.producer.stop()
        if self.websocket_session:
            await self.websocket_session.close()

        logger.info("✓ Producer stopped")

    async def _stream_trades(self) -> None:
        """
        Connect to Binance WebSocket and stream trades.
        Implements reconnection logic with exponential backoff.
        """
        while self.is_running:
            try:
                await self._connect_and_stream()
            except Exception as e:
                logger.error(f"Stream error: {e}. Reconnecting...")
                self.retry_count += 1

                if self.retry_count > self.max_retries:
                    logger.error("Max retries exceeded. Stopping producer.")
                    self.is_running = False
                    break

                # Exponential backoff: 2^retry_count seconds, max 60 seconds
                wait_time = min(2 ** self.retry_count, 60)
                logger.info(f"Retrying in {wait_time}s (attempt {self.retry_count}/{self.max_retries})")
                await asyncio.sleep(wait_time)

        logger.info("Trade streaming stopped")

    async def _connect_and_stream(self) -> None:
        """Connect to Binance WebSocket and process trades."""
        # Build stream name: btcusdt@trade/ethusdt@trade/solusdt@trade
        streams = "/".join([f"{symbol.lower()}@trade" for symbol in self.symbols])
        url = f"{settings.binance_stream_url}/{streams}"

        logger.info(f"Connecting to Binance stream: {streams}")

        try:
            async with self.websocket_session.ws_connect(url, timeout=aiohttp.ClientTimeout(total=60)) as ws:
                self.retry_count = 0  # Reset retry count on successful connection
                logger.info("✓ Connected to Binance WebSocket")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._process_trade_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"WebSocket error: {ws.exception()}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        logger.info("WebSocket closed")
                        break
        except asyncio.TimeoutError:
            logger.error("WebSocket connection timeout")
            raise
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            raise

    async def _process_trade_message(self, message: str) -> None:
        """
        Process a single trade message from Binance.
        
        Binance format:
        {
            "e": "trade",
            "E": 1699564800000,
            "s": "BTCUSDT",
            "t": 123456,
            "p": "45000.50",
            "q": "0.5",
            "b": 123,
            "a": 124,
            "T": 1699564800000,
            "m": false,
            "M": true
        }
        """
        try:
            data = json.loads(message)

            # Extract and validate
            trade = TradeMessage(
                symbol=data.get("s"),
                price=float(data.get("p", 0)),
                quantity=float(data.get("q", 0)),
                side="BUY" if not data.get("m") else "SELL",  # Buyer=True means seller initiated
                timestamp=data.get("T"),
                event_time=datetime.fromtimestamp(data.get("E") / 1000),
                trade_id=data.get("t"),
            )

            # Publish to Kafka
            await self._publish_trade(trade)

        except ValidationError as e:
            logger.warning(f"Invalid trade message: {e}")
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse trade message: {message}")
        except Exception as e:
            logger.error(f"Error processing trade message: {e}")

    async def _publish_trade(self, trade: TradeMessage) -> None:
        """
        Publish trade to Kafka topic.
        
        Partitioning by symbol ensures:
        - Same symbol trades stay in order
        - Different symbols can be processed in parallel
        """
        try:
            if not self.producer:
                logger.error("Producer not initialized")
                return

            # Convert to dict for JSON serialization
            trade_dict = trade.model_dump(mode="json")

            # Publish with symbol as key for partition determinism
            await self.producer.send_and_wait(
                "crypto-trades",
                value=trade_dict,
                key=trade.symbol.encode("utf-8"),  # Partition by symbol
                timestamp_ms=int(trade.timestamp),
            )

            logger.debug(f"Published: {trade.symbol} @ {trade.price}")

        except Exception as e:
            logger.error(f"Failed to publish trade: {e}")


# Global producer instance
producer_instance: Optional[BinanceProducer] = None


async def get_producer() -> BinanceProducer:
    """Get or create global producer instance."""
    global producer_instance
    if producer_instance is None:
        producer_instance = BinanceProducer()
    return producer_instance
