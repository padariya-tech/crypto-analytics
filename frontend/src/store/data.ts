import { create } from 'zustand';
import type { Alert, Analytics, Trade } from '../types';

interface DataState {
  latestPrices: Record<string, number>;
  analytics: Record<string, Analytics>;
  trades: Trade[];
  alerts: Alert[];
  selectedSymbols: string[];
  isDarkMode: boolean;
  isPaused: boolean;
  updatePrice: (symbol: string, price: number) => void;
  updateAnalytics: (analytics: Analytics) => void;
  addTrade: (trade: Trade) => void;
  addAlert: (alert: Alert) => void;
  toggleDarkMode: () => void;
  togglePause: () => void;
  setSelectedSymbols: (symbols: string[]) => void;
  clearData: () => void;
}

const MAX_TRADES = 200;
const MAX_ALERTS = 100;

export const useDataStore = create<DataState>((set, get) => ({
  latestPrices: {},
  analytics: {},
  trades: [],
  alerts: [],
  selectedSymbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
  isDarkMode: true,
  isPaused: false,

  updatePrice: (symbol, price) => {
    if (get().isPaused || !symbol || typeof price !== 'number') {
      return;
    }

    set((state) => ({
      latestPrices: {
        ...state.latestPrices,
        [symbol]: price,
      },
    }));
  },

  updateAnalytics: (analytics) => {
    if (get().isPaused || !analytics.symbol) {
      return;
    }

    set((state) => ({
      analytics: {
        ...state.analytics,
        [analytics.symbol]: analytics,
      },
    }));
  },

  addTrade: (trade) => {
    if (get().isPaused) {
      return;
    }

    set((state) => ({
      trades: [trade, ...state.trades].slice(0, MAX_TRADES),
    }));
  },

  addAlert: (alert) => {
    if (get().isPaused) {
      return;
    }

    set((state) => ({
      alerts: [alert, ...state.alerts].slice(0, MAX_ALERTS),
    }));
  },

  toggleDarkMode: () => {
    set((state) => ({ isDarkMode: !state.isDarkMode }));
  },

  togglePause: () => {
    set((state) => ({ isPaused: !state.isPaused }));
  },

  setSelectedSymbols: (symbols) => {
    set({ selectedSymbols: symbols });
  },

  clearData: () => {
    set({
      latestPrices: {},
      analytics: {},
      trades: [],
      alerts: [],
    });
  },
}));
