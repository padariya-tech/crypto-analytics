import { useMemo } from 'react';
import { useDataStore } from '../store/data';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

export default function Dashboard() {
  const { latestPrices, analytics, alerts, selectedSymbols } = useDataStore();

  // Prepare chart data
  const chartData = useMemo(() => {
    return selectedSymbols.map(symbol => ({
      symbol,
      price: latestPrices[symbol] || 0,
      ma: analytics[symbol]?.moving_average_20 || 0,
      volume: analytics[symbol]?.total_volume || 0,
      change: analytics[symbol]?.price_change_percent || 0,
    }));
  }, [selectedSymbols, latestPrices, analytics]);

  const priceData = selectedSymbols.map(symbol => ({
    name: symbol.replace('USDT', ''),
    price: latestPrices[symbol] || 0,
  }));

  return (
    <div className="space-y-6">
      {/* Key Metrics Grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {selectedSymbols.map(symbol => {
          const data = analytics[symbol];
          const price = latestPrices[symbol] || 0;
          const change = data?.price_change_percent || 0;

          return (
            <div
              key={symbol}
              className="rounded-lg bg-gradient-to-br from-dark-surface to-dark-surface/50 border border-dark-border p-6 hover:border-dark-border/80 transition"
            >
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="text-sm text-gray-400 font-medium">{symbol}</h3>
                  <div className="text-3xl font-bold mt-2">
                    ${price.toLocaleString('en-US', { maximumFractionDigits: 2 })}
                  </div>
                </div>
                <div className={`text-2xl font-bold ${change >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                  {change >= 0 ? '▲' : '▼'} {Math.abs(change).toFixed(2)}%
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <p className="text-gray-500 text-xs">24h High</p>
                  <p className="font-semibold">${data?.high_price?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || '-'}</p>
                </div>
                <div>
                  <p className="text-gray-500 text-xs">24h Low</p>
                  <p className="font-semibold">${data?.low_price?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || '-'}</p>
                </div>
                <div>
                  <p className="text-gray-500 text-xs">Vol/Min</p>
                  <p className="font-semibold">{data?.trades_per_minute || 0}</p>
                </div>
                <div>
                  <p className="text-gray-500 text-xs">MA20</p>
                  <p className="font-semibold">${data?.moving_average_20?.toLocaleString('en-US', { maximumFractionDigits: 2 }) || '-'}</p>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Price Chart */}
        <div className="rounded-lg bg-dark-surface border border-dark-border p-6">
          <h3 className="text-lg font-semibold mb-4">Current Prices</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={priceData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3748" />
              <XAxis dataKey="name" stroke="#9ca3af" />
              <YAxis stroke="#9ca3af" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1a202c', border: '1px solid #2d3748' }}
                labelStyle={{ color: '#fff' }}
              />
              <Bar dataKey="price" fill="#3b82f6" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Volume Trend */}
        <div className="rounded-lg bg-dark-surface border border-dark-border p-6">
          <h3 className="text-lg font-semibold mb-4">Volume Trend</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3748" />
              <XAxis dataKey="symbol" stroke="#9ca3af" />
              <YAxis stroke="#9ca3af" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1a202c', border: '1px solid #2d3748' }}
                labelStyle={{ color: '#fff' }}
              />
              <Legend />
              <Line type="monotone" dataKey="volume" stroke="#10b981" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Recent Alerts */}
      <div className="rounded-lg bg-dark-surface border border-dark-border p-6">
        <h3 className="text-lg font-semibold mb-4">Recent Alerts ({alerts.length})</h3>
        {alerts.length === 0 ? (
          <p className="text-gray-500 py-8 text-center">No alerts yet</p>
        ) : (
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {alerts.slice(0, 10).map(alert => (
              <div
                key={alert.alert_id}
                className={`flex items-start gap-3 p-3 rounded-lg border ${
                  alert.severity === 'HIGH'
                    ? 'bg-accent-red/10 border-accent-red/30'
                    : alert.severity === 'MEDIUM'
                    ? 'bg-accent-yellow/10 border-accent-yellow/30'
                    : 'bg-accent-blue/10 border-accent-blue/30'
                }`}
              >
                <span className={`text-xl mt-0.5 ${
                  alert.severity === 'HIGH'
                    ? 'text-accent-red'
                    : alert.severity === 'MEDIUM'
                    ? 'text-accent-yellow'
                    : 'text-accent-blue'
                }`}>
                  {alert.alert_type === 'PRICE_CHANGE' ? '💹' : '📊'}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="font-semibold text-sm">{alert.message}</p>
                  <p className="text-xs text-gray-400 mt-1">
                    {new Date(alert.timestamp).toLocaleTimeString()}
                  </p>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}