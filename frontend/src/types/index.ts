export interface Trade {
  symbol: string;
  price: number;
  quantity: number;
  side: 'BUY' | 'SELL';
  timestamp: number;
  trade_id: number;
}

export interface Analytics {
  symbol: string;
  current_price: number;
  moving_average_20: number;
  price_change_percent: number;
  trades_per_minute: number;
  total_volume: number;
  high_price: number;
  low_price: number;
  timestamp: string;
}

export interface Alert {
  alert_id: string;
  symbol: string;
  alert_type: 'PRICE_CHANGE' | 'VOLUME_SPIKE' | 'RAPID_MOVEMENT';
  severity: 'LOW' | 'MEDIUM' | 'HIGH';
  message: string;
  current_price: number;
  change_value: number;
  timestamp: string;
}

export interface WebSocketMessage {
  type: 'heartbeat' | 'update' | 'alert' | 'acknowledgement';
  data?: any;
  topic?: string;
  timestamp?: number;
}

export interface MarketData {
  symbol: string;
  price: number;
  change24h: number;
  volume: number;
  lastUpdate: string;
}