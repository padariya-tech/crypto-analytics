"""
Crypto Market Analytics - FastAPI Backend
Real-time WebSocket server and REST APIs for the analytics system.
"""

import asyncio
import logging
import signal
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from config import settings
from producer.binance_producer import get_producer
from consumers.analytics_consumer import get_analytics_consumer
from consumers.alert_consumer import get_alert_engine
from consumers.storage_consumer import get_storage_consumer
from websocket.manager import get_manager
from api.routes import router as api_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Track active tasks for graceful shutdown
active_tasks = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager.
    Handles startup and shutdown of background services.
    """
    logger.info("🚀 Starting Crypto Analytics System...")

    try:
        # Start Kafka Producer (ingests trades from Binance)
        producer = await get_producer()
        await producer.start()
        active_tasks.append(asyncio.current_task())

        # Start Kafka Consumers
        analytics_consumer = await get_analytics_consumer()
        await analytics_consumer.start()

        alert_engine = await get_alert_engine()
        await alert_engine.start()

        storage_consumer = await get_storage_consumer()
        await storage_consumer.start()

        # Start WebSocket heartbeat
        manager = get_manager()
        heartbeat_task = asyncio.create_task(manager.heartbeat())
        active_tasks.append(heartbeat_task)

        logger.info("✓ All services started successfully")

        yield  # App runs here

    except Exception as e:
        logger.error(f"Failed to start services: {e}")
        raise

    finally:
        logger.info("🛑 Shutting down services...")

        try:
            # Stop producer
            producer = await get_producer()
            await producer.stop()

            # Stop consumers
            analytics_consumer = await get_analytics_consumer()
            await analytics_consumer.stop()

            alert_engine = await get_alert_engine()
            await alert_engine.stop()

            storage_consumer = await get_storage_consumer()
            await storage_consumer.stop()

            # Cancel tasks
            for task in active_tasks:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            logger.info("✓ All services shut down gracefully")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


# Create FastAPI app
app = FastAPI(
    title="Crypto Market Analytics API",
    description="Real-time cryptocurrency market analysis with Kafka streaming",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router)


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring."""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "services": {
            "producer": "running",
            "consumers": "running",
            "websocket": "ready",
        },
    }


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API documentation."""
    return {
        "name": "Crypto Market Analytics System",
        "version": "1.0.0",
        "docs": "/docs",
        "websocket": "/ws",
        "status": "running",
    }


if __name__ == "__main__":
    import uvicorn

    logger.info(f"Starting server on {settings.api_host}:{settings.api_port}")

    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_debug,
        log_level="info",
    )