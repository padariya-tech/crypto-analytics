import { useEffect, useRef, useCallback, useState } from 'react';
import { WebSocketMessage } from '../types';

interface WebSocketOptions {
  url: string;
  onMessage?: (data: WebSocketMessage) => void;
  onConnect?: () => void;
  onDisconnect?: () => void;
  onError?: (error: Event) => void;
  reconnectAttempts?: number;
  reconnectDelay?: number;
}

export function useWebSocket(options: WebSocketOptions) {
  const {
    url,
    onMessage,
    onConnect,
    onDisconnect,
    onError,
    reconnectAttempts = 5,
    reconnectDelay = 3000,
  } = options;

  const ws = useRef<WebSocket | null>(null);
  const reconnectCount = useRef(0);
  const reconnectTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [isConnected, setIsConnected] = useState(false);

  const connect = useCallback(() => {
    try {
      ws.current = new WebSocket(url);

      ws.current.onopen = () => {
        setIsConnected(true);
        reconnectCount.current = 0;
        onConnect?.();
      };

      ws.current.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          onMessage?.(data);
        } catch (e) {
          console.error('Failed to parse WebSocket message:', e);
        }
      };

      ws.current.onerror = (error) => {
        console.error('WebSocket error:', error);
        onError?.(error);
      };

      ws.current.onclose = () => {
        setIsConnected(false);
        onDisconnect?.();

        // Attempt reconnection
        if (reconnectCount.current < reconnectAttempts) {
          reconnectCount.current++;
          reconnectTimeout.current = setTimeout(
            connect,
            reconnectDelay * Math.pow(2, reconnectCount.current - 1)
          );
        }
      };
    } catch (error) {
      console.error('Failed to create WebSocket:', error);
    }
  }, [url, onMessage, onConnect, onDisconnect, onError, reconnectAttempts, reconnectDelay]);

  useEffect(() => {
    connect();

    return () => {
      if (reconnectTimeout.current) {
        clearTimeout(reconnectTimeout.current);
      }
      if (ws.current) {
        ws.current.close();
      }
    };
  }, [connect]);

  const send = useCallback((data: any) => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify(data));
    }
  }, []);

  const subscribe = useCallback((topic: string, symbol?: string) => {
    send({
      action: 'subscribe',
      topic,
      symbol,
    });
  }, [send]);

  const unsubscribe = useCallback((topic: string, symbol?: string) => {
    send({
      action: 'unsubscribe',
      topic,
      symbol,
    });
  }, [send]);

  return {
    isConnected,
    send,
    subscribe,
    unsubscribe,
    ws: ws.current,
  };
}
