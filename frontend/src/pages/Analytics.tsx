import { useMemo } from 'react';
import { useDataStore } from '../store/data';
import { Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ComposedChart, Bar } from 'recharts';

export default function Analytics() {
  const { latestPrices, analytics, selectedSymbols } = useDataStore();

  // Prepare analytics data
  const analyticsData = useMemo(() => {
    return selectedSymbols.map(symbol => ({
      symbol: symbol.replace('USDT', ''),
      price: latestPrices[symbol] || 0,
      ma: analytics[symbol]?.moving_average_20 || 0,
      change: analytics[symbol]?.price_change_percent || 0,
      volume: analytics[symbol]?.total_volume || 0,
      trades: analytics[symbol]?.trades_per_minute || 0,
      volatility: Math.abs((analytics[symbol]?.high_price || 0) - (analytics[symbol]?.low_price || 0)),
    }));
  }, [selectedSymbols, latestPrices, analytics]);

  const volumeData = selectedSymbols.map(symbol => ({
    symbol: symbol.replace('USDT', ''),
    volume: analytics[symbol]?.total_volume || 0,
    trades: analytics[symbol]?.trades_per_minute || 0,
  }));

  const volatilityData = selectedSymbols.map(symbol => {
    const data = analytics[symbol];
    const volatility = data ? ((data.high_price - data.low_price) / data.current_price * 100) : 0;
    return {
      symbol: symbol.replace('USDT', ''),
      volatility: volatility.toFixed(2),
      ma: data?.moving_average_20 || 0,
      current: data?.current_price || 0,
    };
  });

  return (
    <div className="space-y-6">
      {/* Analytics Overview */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {analyticsData.map(data => (
          <div key={data.symbol} className="rounded-lg bg-dark-surface border border-dark-border p-4">
            <p className="text-gray-400 text-xs font-semibold uppercase mb-2">{data.symbol}</p>
            <div className="space-y-2">
              <div>
                <p className="text-gray-500 text-xs">Price</p>
                <p className="text-lg font-bold">${data.price.toFixed(2)}</p>
              </div>
              <div>
                <p className="text-gray-500 text-xs">MA20</p>
                <p className="text-sm font-semibold text-accent-blue">${data.ma.toFixed(2)}</p>
              </div>
              <div>
                <p className="text-gray-500 text-xs">Change</p>
                <p className={`text-sm font-semibold ${data.change >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                  {data.change >= 0 ? '+' : ''}{data.change.toFixed(2)}%
                </p>
              </div>
              <div>
                <p className="text-gray-500 text-xs">Volatility</p>
                <p className="text-sm font-semibold text-accent-yellow">{Math.abs(data.change).toFixed(2)}%</p>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Moving Average vs Price */}
        <div className="rounded-lg bg-dark-surface border border-dark-border p-6">
          <h3 className="text-lg font-semibold mb-4">Price vs Moving Average (MA20)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={analyticsData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3748" />
              <XAxis dataKey="symbol" stroke="#9ca3af" />
              <YAxis stroke="#9ca3af" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1a202c', border: '1px solid #2d3748' }}
                labelStyle={{ color: '#fff' }}
              />
              <Legend />
              <Line type="monotone" dataKey="price" stroke="#3b82f6" strokeWidth={2} name="Current Price" />
              <Line type="monotone" dataKey="ma" stroke="#10b981" strokeWidth={2} strokeDasharray="5 5" name="MA20" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* Volume & Trades */}
        <div className="rounded-lg bg-dark-surface border border-dark-border p-6">
          <h3 className="text-lg font-semibold mb-4">Volume & Trade Activity</h3>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={volumeData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3748" />
              <XAxis dataKey="symbol" stroke="#9ca3af" />
              <YAxis yAxisId="left" stroke="#9ca3af" />
              <YAxis yAxisId="right" orientation="right" stroke="#9ca3af" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1a202c', border: '1px solid #2d3748' }}
                labelStyle={{ color: '#fff' }}
              />
              <Legend />
              <Bar yAxisId="left" dataKey="volume" fill="#f59e0b" radius={[8, 8, 0, 0]} name="Volume" />
              <Line yAxisId="right" type="monotone" dataKey="trades" stroke="#3b82f6" name="Trades/Min" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Volatility Analysis */}
      <div className="rounded-lg bg-dark-surface border border-dark-border p-6">
        <h3 className="text-lg font-semibold mb-4">Volatility Analysis</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-dark-border">
                <th className="text-left py-3 px-4 text-gray-400">Symbol</th>
                <th className="text-right py-3 px-4 text-gray-400">Current Price</th>
                <th className="text-right py-3 px-4 text-gray-400">MA20</th>
                <th className="text-right py-3 px-4 text-gray-400">Volatility %</th>
                <th className="text-right py-3 px-4 text-gray-400">Status</th>
              </tr>
            </thead>
            <tbody>
              {volatilityData.map(data => {
                const volatility = parseFloat(data.volatility);
                const isVolatile = volatility > 5;
                const isPriceAboveMA = data.current > data.ma;

                return (
                  <tr key={data.symbol} className="border-b border-dark-border/50 hover:bg-dark-border/20 transition">
                    <td className="py-3 px-4 font-semibold text-accent-blue">{data.symbol}</td>
                    <td className="text-right py-3 px-4 font-mono">${data.current.toFixed(2)}</td>
                    <td className="text-right py-3 px-4 font-mono">${data.ma.toFixed(2)}</td>
                    <td className={`text-right py-3 px-4 font-semibold ${
                      isVolatile ? 'text-accent-red' : 'text-accent-green'
                    }`}>
                      {data.volatility}%
                    </td>
                    <td className="text-right py-3 px-4">
                      <span className={`px-2 py-1 rounded text-xs font-semibold ${
                        isVolatile ? 'bg-accent-red/20 text-accent-red' : 'bg-accent-green/20 text-accent-green'
                      }`}>
                        {isVolatile ? 'High Vol' : 'Normal'} {isPriceAboveMA ? '📈' : '📉'}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Performance Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="rounded-lg bg-gradient-to-br from-dark-surface to-dark-surface/50 border border-dark-border p-6">
          <h4 className="text-gray-400 text-sm font-semibold mb-3">Best Performer</h4>
          <p className="text-2xl font-bold">
            {analyticsData.reduce((best, curr) => 
              curr.change > best.change ? curr : best
            ).symbol}
          </p>
          <p className={`text-sm mt-2 ${
            analyticsData.reduce((best, curr) => 
              curr.change > best.change ? curr : best
            ).change >= 0 ? 'text-accent-green' : 'text-accent-red'
          }`}>
            {analyticsData.reduce((best, curr) => 
              curr.change > best.change ? curr : best
            ).change.toFixed(2)}%
          </p>
        </div>

        <div className="rounded-lg bg-gradient-to-br from-dark-surface to-dark-surface/50 border border-dark-border p-6">
          <h4 className="text-gray-400 text-sm font-semibold mb-3">Highest Volume</h4>
          <p className="text-2xl font-bold">
            {volumeData.reduce((best, curr) => 
              curr.volume > best.volume ? curr : best
            ).symbol}
          </p>
          <p className="text-sm text-gray-400 mt-2">
            {(volumeData.reduce((best, curr) => 
              curr.volume > best.volume ? curr : best
            ).volume / 1000000).toFixed(2)}M
          </p>
        </div>

        <div className="rounded-lg bg-gradient-to-br from-dark-surface to-dark-surface/50 border border-dark-border p-6">
          <h4 className="text-gray-400 text-sm font-semibold mb-3">Avg Volatility</h4>
          <p className="text-2xl font-bold">
            {(analyticsData.reduce((sum, curr) => sum + Math.abs(curr.change), 0) / analyticsData.length).toFixed(2)}%
          </p>
          <p className="text-sm text-accent-blue mt-2">Across all symbols</p>
        </div>
      </div>
    </div>
  );
}
