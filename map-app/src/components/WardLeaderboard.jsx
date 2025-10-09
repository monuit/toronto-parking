import { useMemo } from 'react';
import { formatCurrency, formatNumber } from '../lib/dataTransforms';
import '../styles/Leaderboard.css';

export function WardLeaderboard({
  items = [],
  loading = false,
  onWardSelect,
  dataset = 'red_light_locations',
}) {
  const memoisedItems = useMemo(() => {
    if (!Array.isArray(items)) {
      return [];
    }
    return items.slice(0, 10);
  }, [items]);

  if (loading) {
    return (
      <div className="leaderboard ward-leaderboard">
        <div className="loading">Loading...</div>
      </div>
    );
  }

  return (
    <div className="leaderboard ward-leaderboard">
      <p className="subtitle">
        Ward rankings by traffic enforcement — {dataset === 'cameras_combined' ? 'combined red light + ASE totals' : 'tickets issued'}.
      </p>
      <div className="leaderboard-list">
        {memoisedItems.map((item, index) => (
          <div
            key={item.wardCode ?? item.wardName ?? index}
            className="leaderboard-item clickable"
            onClick={() => onWardSelect?.(item)}
          >
            <div className="rank">{index + 1}</div>
            <div className="details">
              <div className="name">{item.wardName || item.name || `Ward ${item.wardCode ?? ''}`}</div>
              <div className="stats">
                <span className="ticket-count">{formatNumber(item.ticketCount ?? 0)} tickets</span>
                {Number.isFinite(item.totalRevenue) && item.totalRevenue > 0 ? (
                  <span className="revenue">{formatCurrency(item.totalRevenue)}</span>
                ) : null}
                {Number.isFinite(item.locationCount) && item.locationCount > 0 ? (
                  <span className="extra">{formatNumber(item.locationCount)} locations</span>
                ) : null}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default WardLeaderboard;
