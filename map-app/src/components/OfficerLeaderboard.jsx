/**
 * OfficerLeaderboard - Display top officers by ticket count
 * Single responsibility: render officer statistics in ranked list
 */
import { useEffect, useState } from 'react';
import { formatNumber, formatCurrency } from '../lib/dataTransforms';
import { MAP_CONFIG } from '../lib/mapSources';
import '../styles/Leaderboard.css';

export function OfficerLeaderboard({ visible = true }) {
  const [officers, setOfficers] = useState([]);
  const [loading, setLoading] = useState(true);
  
  useEffect(() => {
    fetch(MAP_CONFIG.DATA_PATHS.OFFICER_STATS)
      .then(res => res.json())
      .then(data => {
        const sorted = Object.entries(data)
          .map(([id, stats]) => ({ id, ...stats }))
          .sort((a, b) => b.ticketCount - a.ticketCount)
          .slice(0, 10);
        setOfficers(sorted);
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to load officer stats:', err);
        setLoading(false);
      });
  }, []);
  
  if (!visible) return null;
  
  return (
    <div className="leaderboard officer-leaderboard">
      <h2>🎖️ Top Officers</h2>
      <p className="subtitle">Most tickets issued (2008-2024)</p>
      
      {loading ? (
        <div className="loading">Loading...</div>
      ) : (
        <div className="leaderboard-list">
          {officers.map((officer, index) => (
            <div key={officer.id} className="leaderboard-item">
              <div className="rank">#{index + 1}</div>
              <div className="details">
                <div className="name">Officer {officer.id}</div>
                <div className="stats">
                  <span className="ticket-count">
                    {formatNumber(officer.ticketCount)} tickets
                  </span>
                  <span className="revenue">
                    {formatCurrency(officer.totalRevenue)}
                  </span>
                </div>
                {officer.topInfraction && (
                  <div className="top-infraction">
                    Most common: Code {officer.topInfraction}
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
