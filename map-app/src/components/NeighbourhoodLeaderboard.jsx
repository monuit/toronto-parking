/**
 * NeighbourhoodLeaderboard - Display top neighbourhoods by ticket count
 * Single responsibility: render neighbourhood statistics in ranked list
 */
import { useEffect, useMemo, useState } from 'react';
import { formatNumber, formatCurrency } from '../lib/dataTransforms';
import { useAppData } from '../context/AppDataContext.jsx';
import { MAP_CONFIG } from '../lib/mapSources';
import '../styles/Leaderboard.css';

export function NeighbourhoodLeaderboard({ visible = true, onNeighbourhoodClick, initialNeighbourhoods = [] }) {
  const { totals, topNeighbourhoods: contextNeighbourhoods = [] } = useAppData();
  const filteredInitial = useMemo(
    () => initialNeighbourhoods.filter((hood) => hood?.name && hood.name !== 'Unknown'),
    [initialNeighbourhoods],
  );
  const filteredContext = useMemo(
    () => contextNeighbourhoods.filter((hood) => hood?.name && hood.name !== 'Unknown'),
    [contextNeighbourhoods],
  );
  const [neighbourhoods, setNeighbourhoods] = useState(
    () => (filteredContext.length > 0 ? filteredContext : filteredInitial),
  );
  const [loading, setLoading] = useState(() => (filteredContext.length === 0 && filteredInitial.length === 0));

  const summaryCopy = useMemo(() => {
    const totalTickets = totals?.ticketCount ? formatNumber(totals.ticketCount) : null;
    const totalRevenue = totals?.totalRevenue ? formatCurrency(totals.totalRevenue) : null;

    if (totalTickets && totalRevenue) {
      return `Ranking neighbourhoods by total fines — ${totalTickets} tickets adding up to ${totalRevenue}.`;
    }
    if (totalTickets) {
      return `Ranking neighbourhoods by total fines — ${totalTickets} tickets issued across the city.`;
    }
    if (totalRevenue) {
      return `Ranking neighbourhoods by total fines — ${totalRevenue} collected across Toronto.`;
    }
    return 'Ranking neighbourhoods by total fines issued across Toronto.';
  }, [totals]);

  useEffect(() => {
    if (filteredContext.length > 0) {
      setNeighbourhoods(filteredContext);
      setLoading(false);
    } else if (filteredInitial.length > 0) {
      setNeighbourhoods(filteredInitial);
      setLoading(false);
    }
  }, [filteredContext, filteredInitial]);

  useEffect(() => {
    if (filteredContext.length > 0 || filteredInitial.length > 0) {
      return;
    }

    fetch(MAP_CONFIG.DATA_PATHS.NEIGHBOURHOOD_STATS)
      .then(res => res.json())
      .then(data => {
        const sorted = Object.entries(data)
          .map(([name, stats]) => ({ name, ...stats }))
          .filter((hood) => hood.name && hood.name !== 'Unknown')
          .sort((a, b) => b.totalFines - a.totalFines)
          .slice(0, 10);
        setNeighbourhoods(sorted);
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to load neighbourhood stats:', err);
        setLoading(false);
      });
  }, [filteredContext, filteredInitial]);

  const handleClick = (name) => {
    if (onNeighbourhoodClick) {
      onNeighbourhoodClick(name);
    }
  };

  if (!visible) return null;

  return (
    <div className="leaderboard neighbourhood-leaderboard">
      <p className="subtitle">{summaryCopy}</p>

      {loading ? (
        <div className="loading">Loading...</div>
      ) : (
        <div className="leaderboard-list">
          {neighbourhoods.map((hood, index) => (
            <div
              key={hood.name}
              className="leaderboard-item clickable"
              onClick={() => handleClick(hood.name)}
            >
              <div className="rank">{index + 1}</div>
              <div className="details">
                <div className="name">{hood.name}</div>
                <div className="stats">
                  <span className="ticket-count">
                    {formatNumber(hood.count)} tickets
                  </span>
                  <span className="revenue">
                    {formatCurrency(hood.totalFines)}
                  </span>
                </div>
                {hood.topInfraction && (
                  <div className="top-infraction">
                    Most common: Code {hood.topInfraction}
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
