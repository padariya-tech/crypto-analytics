"""
REST API Endpoints
Provides access to market data, analytics, alerts, and system information.
"""

import logging
from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from pydantic import BaseModel

from consumers.storage_consumer import get_storage_consumer
from websocket.manager import get_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["analytics"])


# ============================================================================
# Response Models
# ============================================================================


class PriceData(BaseModel):
    """Current price for a symbol."""

    symbol: str
    price: float
    timestamp: str


class AnalyticsData(BaseModel):
    """Analytics metrics for a symbol."""

    symbol: str
    current_price: float
    moving_average_20: Optional[float] = None
    price_change_percent: float
    trades_per_minute: int
    total_volume: float
    timestamp: str


class Alert(BaseModel):
    """Price alert."""

    alert_id: str
    symbol: str
    alert_type: str
    severity: str
    message: str
    current_price: float
    timestamp: str


class SystemStats(BaseModel):
    """System status and metrics."""

    status: str
    uptime_seconds: int
    total_connections: int
    symbols_monitored: List[str]
    topics: List[str]


# ============================================================================
# REST Endpoints
# ============================================================================


@router.get("/prices", response_model=List[PriceData])
async def get_prices(
    symbols: Optional[str] = None,
    storage=Depends(get_storage_consumer),
):
    """
    Get latest prices for symbols.
    
    Query parameters:
    - symbols: Comma-separated list (e.g., "BTCUSDT,ETHUSDT")
    
    Returns latest cached prices from Redis.
    """
    try:
        if symbols:
            symbol_list = symbols.split(",")
        else:
            from config import settings
            symbol_list = settings.symbols

        prices = []
        for symbol in symbol_list:
            price = await storage.get_latest_price(symbol)
            if price:
                prices.append(
                    PriceData(
                        symbol=symbol,
                        price=price,
                        timestamp=datetime.now().isoformat(),
                    )
                )

        return prices

    except Exception as e:
        logger.error(f"Error fetching prices: {e}")
        return []


@router.get("/alerts", response_model=List[Alert])
async def get_recent_alerts(
    limit: int = 50,
    storage=Depends(get_storage_consumer),
):
    """
    Get recent price alerts.
    
    Query parameters:
    - limit: Maximum number of alerts to return (default: 50)
    
    Returns alerts stored in Redis (ordered by recency).
    """
    try:
        alerts = await storage.get_recent_alerts(limit)
        return [
            Alert(
                alert_id=a.get("alert_id", ""),
                symbol=a.get("symbol", ""),
                alert_type=a.get("type", ""),
                severity=a.get("severity", ""),
                message=a.get("message", ""),
                current_price=a.get("current_price", 0),
                timestamp=a.get("timestamp", ""),
            )
            for a in alerts
        ]

    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        return []


@router.get("/stats", response_model=SystemStats)
async def get_system_stats():
    """
    Get system statistics and status.
    
    Returns:
    - System status
    - Active WebSocket connections
    - Monitored symbols
    - Kafka topics
    """
    from config import settings

    manager = get_manager()
    stats = manager.get_stats()

    return SystemStats(
        status="healthy",
        uptime_seconds=0,  # Would need startup timestamp
        total_connections=stats.get("total_connections", 0),
        symbols_monitored=settings.symbols,
        topics=[
            "crypto-trades",
            "processed-market-data",
            "price-alerts",
            "anomalies",
        ],
    )


@router.get("/config")
async def get_config():
    """
    Get system configuration.
    
    Returns alert thresholds and processing parameters.
    """
    from config import settings

    return {
        "symbols": settings.symbols,
        "alerts": {
            "price_change_threshold": settings.price_change_threshold,
            "volume_spike_threshold": settings.volume_spike_threshold,
            "rapid_price_movement_threshold": settings.rapid_price_movement_threshold,
        },
        "processing": {
            "analytics_window_size": settings.analytics_window_size,
            "alert_cooldown_seconds": settings.alert_cooldown,
        },
    }


# ============================================================================
# WebSocket Endpoint
# ============================================================================


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates.
    
    Client can:
    - Subscribe to price updates
    - Subscribe to alerts
    - Subscribe to analytics
    
    Message format:
    {
        "action": "subscribe|unsubscribe",
        "topic": "prices|alerts|analytics",
        "symbol": "BTCUSDT"  // optional, for symbol-specific updates
    }
    
    Server sends:
    {
        "type": "heartbeat|update|alert",
        "data": {...},
        "timestamp": 1234567890
    }
    """
    manager = get_manager()

    # Try to connect
    if not await manager.connect(websocket):
        await websocket.close(code=1008, reason="Connection limit reached")
        return

    try:
        while True:
            # Receive message from client
            data = await websocket.receive_json()

            # Handle subscription
            await manager.handle_client_message(websocket, data)

            # Send confirmation
            action = data.get("action")
            topic = data.get("topic")
            if action in ["subscribe", "unsubscribe"]:
                await manager.send_personal_message(
                    websocket,
                    {
                        "action": action,
                        "topic": topic,
                        "status": "confirmed",
                    },
                    "acknowledgement",
                )

    except WebSocketDisconnect:
        await manager.disconnect(websocket)

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await manager.disconnect(websocket)


# ============================================================================
# Data Export Endpoints
# ============================================================================


@router.get("/export/trades/csv")
async def export_trades_csv(
    symbol: Optional[str] = None,
    hours: int = 24,
):
    """
    Export trade data to CSV.
    
    Note: In a real implementation, would stream from database.
    For this demo, returns a sample CSV structure.
    """
    import csv
    from io import StringIO

    output = StringIO()
    writer = csv.DictWriter(
        output, fieldnames=["timestamp", "symbol", "price", "quantity", "side"]
    )
    writer.writeheader()

    # In production: query database for trades in time range
    # For now, return empty CSV with correct format
    return {"format": "csv", "sample": output.getvalue()}


# ============================================================================
# Kafka Monitoring Endpoints
# ============================================================================


@router.get("/kafka/topics")
async def get_kafka_topics():
    """
    Get information about Kafka topics.
    
    Returns topic names, partitions, and purpose.
    """
    return {
        "topics": [
            {
                "name": "crypto-trades",
                "description": "Raw trades from Binance",
                "partitions": "by symbol (BTC, ETH, SOL)",
                "retention": "24 hours",
            },
            {
                "name": "processed-market-data",
                "description": "Calculated analytics (MA, volume, etc)",
                "partitions": "by symbol",
                "retention": "1 hour",
            },
            {
                "name": "price-alerts",
                "description": "Generated price alerts",
                "partitions": "single",
                "retention": "7 days",
            },
            {
                "name": "anomalies",
                "description": "Dead letter queue for errors",
                "partitions": "single",
                "retention": "30 days",
            },
        ]
    }


@router.get("/kafka/consumer-groups")
async def get_consumer_groups():
    """
    Get information about consumer groups.
    
    Returns consumer group names and their purpose.
    """
    return {
        "consumer_groups": [
            {
                "name": "analytics-group",
                "topics": ["crypto-trades"],
                "purpose": "Calculate analytics (moving averages, volume)",
            },
            {
                "name": "alerts-group",
                "topics": ["processed-market-data"],
                "purpose": "Generate price alerts",
            },
            {
                "name": "storage-group",
                "topics": ["processed-market-data", "price-alerts"],
                "purpose": "Persist to Redis and PostgreSQL",
            },
        ]
    }


# ============================================================================
# Admin Endpoints
# ============================================================================


@router.post("/admin/reset-alerts")
async def reset_alert_cooldowns():
    """
    Reset alert cooldown timers.
    
    Use for testing or emergency alert reset.
    Allows alerts to be generated immediately again.
    """
    try:
        from consumers.alert_consumer import alert_engine_instance

        if alert_engine_instance:
            alert_engine_instance.last_alert_times.clear()
            return {"status": "success", "message": "Alert cooldowns reset"}
        else:
            return {"status": "error", "message": "Alert engine not initialized"}

    except Exception as e:
        logger.error(f"Error resetting alerts: {e}")
        return {"status": "error", "message": str(e)}


@router.get("/admin/consumer-lag")
async def get_consumer_lag():
    """
    Get Kafka consumer group lag.
    
    Note: In production, would query Kafka broker directly.
    """
    return {
        "note": "Consumer lag would be fetched from Kafka broker",
        "groups": {
            "analytics-group": {
                "lag": 0,
                "status": "healthy",
            },
            "alerts-group": {
                "lag": 0,
                "status": "healthy",
            },
            "storage-group": {
                "lag": 0,
                "status": "healthy",
            },
        },
    }