"""
WebSocket Manager
Manages client connections and broadcasts real-time updates.
Handles reconnection, heartbeat, and message routing.
"""

import asyncio
import json
import logging
from typing import Set, Optional, Dict, Callable

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages WebSocket connections and broadcasts messages.
    
    Features:
    - Client connection tracking
    - Broadcast to all clients
    - Targeted messages to specific client groups
    - Heartbeat to detect stale connections
    - Graceful reconnection handling
    """

    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.client_metadata: Dict[WebSocket, dict] = {}
        self.message_handlers: Dict[str, Callable] = {}
        self.max_connections = 100

    async def connect(self, websocket: WebSocket, client_id: str = None) -> bool:
        """
        Accept a new WebSocket connection.
        
        Args:
            websocket: The WebSocket connection
            client_id: Optional identifier for the client
            
        Returns:
            True if connection accepted, False if limit reached
        """
        if len(self.active_connections) >= self.max_connections:
            logger.warning(
                f"Connection limit reached ({self.max_connections}). "
                f"Rejecting new connection."
            )
            return False

        try:
            await websocket.accept()
            self.active_connections.add(websocket)

            # Store client metadata
            self.client_metadata[websocket] = {
                "client_id": client_id or f"client_{len(self.active_connections)}",
                "connected_at": asyncio.get_event_loop().time(),
                "subscriptions": set(),
            }

            logger.info(
                f"Client connected: {self.client_metadata[websocket]['client_id']} "
                f"(Total: {len(self.active_connections)})"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to accept connection: {e}")
            return False

    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            client_id = self.client_metadata.pop(websocket, {}).get(
                "client_id", "unknown"
            )
            logger.info(
                f"Client disconnected: {client_id} "
                f"(Remaining: {len(self.active_connections)})"
            )

    async def broadcast(self, message: dict, message_type: str = "update") -> None:
        """
        Broadcast a message to all connected clients.
        
        Args:
            message: Message content (will be JSON serialized)
            message_type: Type of message for client routing
        """
        if not self.active_connections:
            return

        data = {
            "type": message_type,
            "data": message,
            "timestamp": asyncio.get_event_loop().time(),
        }

        dead_connections = set()

        for connection in self.active_connections:
            try:
                await connection.send_json(data)
            except WebSocketDisconnect:
                dead_connections.add(connection)
            except Exception as e:
                logger.warning(f"Failed to send message to client: {e}")
                dead_connections.add(connection)

        # Clean up dead connections
        for conn in dead_connections:
            await self.disconnect(conn)

    async def broadcast_to_subscribers(
        self, topic: str, message: dict, message_type: str = "update"
    ) -> None:
        """Broadcast to clients subscribed to a specific topic."""
        for connection, metadata in self.client_metadata.items():
            if topic in metadata.get("subscriptions", set()):
                try:
                    data = {
                        "type": message_type,
                        "topic": topic,
                        "data": message,
                        "timestamp": asyncio.get_event_loop().time(),
                    }
                    await connection.send_json(data)
                except Exception as e:
                    logger.warning(f"Failed to send message to subscriber: {e}")

    async def send_personal_message(
        self, websocket: WebSocket, message: dict, message_type: str = "update"
    ) -> None:
        """Send a message to a specific client."""
        try:
            data = {
                "type": message_type,
                "data": message,
                "timestamp": asyncio.get_event_loop().time(),
            }
            await websocket.send_json(data)
        except Exception as e:
            logger.warning(f"Failed to send personal message: {e}")
            await self.disconnect(websocket)

    async def handle_client_message(self, websocket: WebSocket, data: dict) -> None:
        """
        Handle messages from client.
        
        Message format:
        {
            "action": "subscribe|unsubscribe|request",
            "topic": "symbol|alerts|analytics",
            "payload": {...}
        }
        """
        action = data.get("action")
        topic = data.get("topic")

        if action == "subscribe":
            self.client_metadata[websocket]["subscriptions"].add(topic)
            logger.info(f"Client subscribed to {topic}")

        elif action == "unsubscribe":
            self.client_metadata[websocket]["subscriptions"].discard(topic)
            logger.info(f"Client unsubscribed from {topic}")

        elif action == "request":
            # Handle specific data requests
            if topic in self.message_handlers:
                handler = self.message_handlers[topic]
                response = await handler(data.get("payload"))
                await self.send_personal_message(websocket, response, "response")

    def register_handler(self, topic: str, handler: Callable) -> None:
        """Register a handler for request-response messages."""
        self.message_handlers[topic] = handler

    async def heartbeat(self, interval: int = 30) -> None:
        """
        Send periodic heartbeat to all clients.
        Detects and removes stale connections.
        """
        while True:
            try:
                await asyncio.sleep(interval)

                data = {
                    "type": "heartbeat",
                    "timestamp": asyncio.get_event_loop().time(),
                }

                dead_connections = set()

                for connection in self.active_connections:
                    try:
                        await connection.send_json(data)
                    except Exception:
                        dead_connections.add(connection)

                # Cleanup
                for conn in dead_connections:
                    await self.disconnect(conn)

                if dead_connections:
                    logger.debug(
                        f"Removed {len(dead_connections)} stale connections"
                    )

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(interval)

    def get_stats(self) -> dict:
        """Get connection statistics."""
        return {
            "total_connections": len(self.active_connections),
            "max_connections": self.max_connections,
            "clients": [
                {
                    "id": metadata.get("client_id"),
                    "subscriptions": list(metadata.get("subscriptions", set())),
                }
                for metadata in self.client_metadata.values()
            ],
        }


# Global connection manager
manager: Optional[ConnectionManager] = None


def get_manager() -> ConnectionManager:
    """Get or create global connection manager."""
    global manager
    if manager is None:
        manager = ConnectionManager()
    return manager