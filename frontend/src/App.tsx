import { useState, useEffect } from 'react';
import { useDataStore } from './store/data';
import { useWebSocket } from './hooks/useWebSocket';
import Dashboard from './pages/Dashboard';
import TradeFeed from './pages/TradeFeed';
import Analytics from './pages/Analytics';

function App() {
  const [currentPage, setCurrentPage] = useState('dashboard');
  const { isDarkMode, toggleDarkMode, isPaused, togglePause, addAlert, addTrade, updatePrice, updateAnalytics } = useDataStore();

  const wsUrl = import.meta.env.VITE_WS_URL || 'ws://localhost:8000/ws';

  const { isConnected, subscribe } = useWebSocket({
    url: wsUrl,
    onMessage: (data) => {
      if (data.type === 'update') {
        const { symbol, current_price, ...rest } = data.data;
        
        if (symbol) {
          updatePrice(symbol, current_price);
          updateAnalytics({ symbol, current_price, ...rest });
          addTrade({
            symbol,
            price: current_price,
            quantity: data.data.quantity || 0,
            side: data.data.side || 'BUY',
            timestamp: Date.now(),
            trade_id: Math.floor(Math.random() * 1000000),
          });
        }
      } else if (data.type === 'alert') {
        addAlert(data.data);
      }
    },
    onConnect: () => {
      console.log('WebSocket connected');
      subscribe('prices');
      subscribe('alerts');
      subscribe('analytics');
    },
  });

  useEffect(() => {
    // Apply dark mode class
    const html = document.documentElement;
    if (isDarkMode) {
      html.classList.add('dark');
    } else {
      html.classList.remove('dark');
    }
  }, [isDarkMode]);

  const navItems = [
    { id: 'dashboard', label: 'Dashboard', icon: '📊' },
    { id: 'feed', label: 'Trade Feed', icon: '📈' },
    { id: 'analytics', label: 'Analytics', icon: '📉' },
  ];

  return (
    <div className={isDarkMode ? 'dark' : ''}>
      <div className="min-h-screen bg-gradient-to-br from-dark-bg via-dark-surface to-dark-bg text-white">
        {/* Header */}
        <header className="sticky top-0 z-50 border-b border-dark-border bg-dark-surface/80 backdrop-blur">
          <div className="max-w-7xl mx-auto px-4 py-4 flex justify-between items-center">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-accent-blue to-blue-600 flex items-center justify-center font-bold">
                ₿
              </div>
              <h1 className="text-xl font-bold">Crypto Analytics</h1>
              <div className="flex items-center gap-2 ml-4">
                <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`} />
                <span className="text-sm text-gray-400">
                  {isConnected ? 'Connected' : 'Disconnected'}
                </span>
              </div>
            </div>

            <div className="flex items-center gap-4">
              <button
                onClick={togglePause}
                className={`px-3 py-1 rounded-lg text-sm font-medium transition ${
                  isPaused
                    ? 'bg-accent-red/20 text-accent-red'
                    : 'bg-accent-green/20 text-accent-green'
                }`}
              >
                {isPaused ? '▶ Resume' : '⏸ Pause'}
              </button>
              <button
                onClick={toggleDarkMode}
                className="p-2 rounded-lg hover:bg-dark-border transition"
              >
                {isDarkMode ? '☀️' : '🌙'}
              </button>
            </div>
          </div>

          {/* Navigation */}
          <nav className="max-w-7xl mx-auto px-4 flex gap-1 border-t border-dark-border">
            {navItems.map(item => (
              <button
                key={item.id}
                onClick={() => setCurrentPage(item.id)}
                className={`px-4 py-3 text-sm font-medium transition border-b-2 ${
                  currentPage === item.id
                    ? 'border-accent-blue text-accent-blue'
                    : 'border-transparent text-gray-400 hover:text-white'
                }`}
              >
                {item.icon} {item.label}
              </button>
            ))}
          </nav>
        </header>

        {/* Main Content */}
        <main className="max-w-7xl mx-auto px-4 py-8">
          {currentPage === 'dashboard' && <Dashboard />}
          {currentPage === 'feed' && <TradeFeed />}
          {currentPage === 'analytics' && <Analytics />}
        </main>

        {/* Footer */}
        <footer className="border-t border-dark-border bg-dark-surface/50 mt-12 py-6 text-center text-sm text-gray-400">
          <p>Real-Time Crypto Market Analytics • Powered by Kafka Streaming</p>
        </footer>
      </div>
    </div>
  );
}

export default App;
