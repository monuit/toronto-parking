/**
 * StreetLeaderboard - Display top streets by ticket count
 * Single responsibility: render street statistics in ranked list
 */
import { useEffect, useMemo, useState } from 'react';
import { formatNumber, formatCurrency } from '../lib/dataTransforms';
import { useAppData } from '../context/AppDataContext.jsx';
import '../styles/Leaderboard.css';

export function StreetLeaderboard({ visible = true, initialStreets = [], onStreetSelect }) {
  const { totals, topStreets = [] } = useAppData();
  const [streets, setStreets] = useState(() => (topStreets.length > 0 ? topStreets : initialStreets));
  const [loading, setLoading] = useState(() => (topStreets.length === 0 && initialStreets.length === 0));

  const summaryCopy = useMemo(() => {
    const totalTickets = totals?.ticketCount ? formatNumber(totals.ticketCount) : null;
    const totalRevenue = totals?.totalRevenue ? formatCurrency(totals.totalRevenue) : null;

    if (totalTickets && totalRevenue) {
      return `Ranking streets by total fines — ${totalTickets} tickets adding up to ${totalRevenue}.`;
    }
    if (totalTickets) {
      return `Ranking streets by total fines — ${totalTickets} tickets issued across the city.`;
    }
    if (totalRevenue) {
      return `Ranking streets by total fines — ${totalRevenue} collected across Toronto.`;
    }
    return 'Ranking streets by total fines issued across Toronto.';
  }, [totals]);

  useEffect(() => {
    if (topStreets.length > 0) {
      setStreets(topStreets);
      setLoading(false);
    } else if (initialStreets.length > 0) {
      setStreets(initialStreets);
      setLoading(false);
    }
  }, [topStreets, initialStreets]);

  useEffect(() => {
    if (topStreets.length > 0 || initialStreets.length > 0) {
      return;
    }

    fetch('/data/street_stats.json')
      .then(res => res.json())
      .then(data => {
        const sorted = Object.entries(data)
          .map(([name, stats]) => ({ name, ...stats }))
          .sort((a, b) => b.totalRevenue - a.totalRevenue)
          .slice(0, 10);
        setStreets(sorted);
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to load street stats:', err);
        setLoading(false);
      });
  }, [topStreets, initialStreets]);

  if (!visible) return null;

  return (
    <div className="leaderboard street-leaderboard">
      <p className="subtitle">{summaryCopy}</p>

      {loading ? (
        <div className="loading">Loading...</div>
      ) : (
        <div className="leaderboard-list">
          {streets.map((street, index) => (
            <button
              key={street.name || street.sampleLocation || street.address || index}
              type="button"
              className="leaderboard-item"
              onClick={() => onStreetSelect?.(street)}
            >
              <div className="rank">{index + 1}</div>
              <div className="details">
                <div className="name">{street.sampleLocation || street.address || street.name}</div>
                <div className="stats">
                  <span className="ticket-count">
                    {formatNumber(street.ticketCount)} tickets
                  </span>
                  <span className="revenue">
                    {formatCurrency(street.totalRevenue)}
                  </span>
                </div>
                {street.topInfraction && (
                  <div className="top-infraction">
                    Most common: Code {parseFloat(street.topInfraction).toString()}
                  </div>
                )}
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
