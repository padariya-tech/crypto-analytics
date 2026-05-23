import { useDataStore } from '../store/data';

export default function TradeFeed() {
  const { trades, isPaused } = useDataStore();

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold">Live Trade Feed</h2>
          <p className="text-gray-400 text-sm mt-1">Real-time trades from Binance</p>
        </div>
        <div className="text-right">
          <p className="text-lg font-semibold">{trades.length} Trades</p>
          <p className="text-sm text-gray-400">
            {isPaused ? '⏸ Paused' : '🟢 Live'}
          </p>
        </div>
      </div>

      {/* Trade List */}
      <div className="rounded-lg bg-dark-surface border border-dark-border overflow-hidden">
        <div className="max-h-96 overflow-y-auto">
          {trades.length === 0 ? (
            <div className="flex items-center justify-center h-64 text-gray-500">
              <p>Waiting for trades...</p>
            </div>
          ) : (
            <div className="divide-y divide-dark-border">
              {/* Header Row */}
              <div className="sticky top-0 grid grid-cols-5 gap-4 px-6 py-3 bg-dark-border/50 text-sm font-semibold text-gray-400">
                <div>Symbol</div>
                <div className="text-right">Price</div>
                <div className="text-right">Quantity</div>
                <div className="text-center">Side</div>
                <div className="text-right">Time</div>
              </div>

              {/* Trade Rows */}
              {trades.map((trade, index) => (
                <div
                  key={`${trade.trade_id}-${index}`}
                  className="grid grid-cols-5 gap-4 px-6 py-3 hover:bg-dark-border/30 transition animate-slide-in"
                >
                  <div className="font-semibold text-accent-blue">{trade.symbol}</div>
                  <div className="text-right font-mono">
                    ${trade.price.toLocaleString('en-US', { maximumFractionDigits: 2 })}
                  </div>
                  <div className="text-right font-mono text-gray-400">
                    {trade.quantity.toFixed(4)}
                  </div>
                  <div className={`text-center font-semibold ${
                    trade.side === 'BUY' ? 'text-accent-green' : 'text-accent-red'
                  }`}>
                    {trade.side === 'BUY' ? '▲ BUY' : '▼ SELL'}
                  </div>
                  <div className="text-right text-sm text-gray-400">
                    {new Date(trade.timestamp).toLocaleTimeString()}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Statistics */}
      <div className="grid grid-cols-3 gap-4">
        <div className="rounded-lg bg-dark-surface border border-dark-border p-4">
          <p className="text-gray-400 text-sm mb-2">Buy Trades</p>
          <p className="text-2xl font-bold text-accent-green">
            {trades.filter(t => t.side === 'BUY').length}
          </p>
        </div>
        <div className="rounded-lg bg-dark-surface border border-dark-border p-4">
          <p className="text-gray-400 text-sm mb-2">Sell Trades</p>
          <p className="text-2xl font-bold text-accent-red">
            {trades.filter(t => t.side === 'SELL').length}
          </p>
        </div>
        <div className="rounded-lg bg-dark-surface border border-dark-border p-4">
          <p className="text-gray-400 text-sm mb-2">Buy/Sell Ratio</p>
          <p className="text-2xl font-bold text-accent-blue">
            {(trades.filter(t => t.side === 'BUY').length / Math.max(trades.filter(t => t.side === 'SELL').length, 1)).toFixed(2)}
          </p>
        </div>
      </div>
    </div>
  );
}