/**
 * NeighbourhoodLeaderboard - Display top neighbourhoods by ticket count
 * Single responsibility: render neighbourhood statistics in ranked list
 */
import { useEffect, useMemo, useState } from 'react';
import { formatNumber, formatCurrency } from '../lib/dataTransforms';
import { useAppData } from '../context/AppDataContext.jsx';
import { MAP_CONFIG } from '../lib/mapSources';
import '../styles/Leaderboard.css';

const SUMMARY_COPY = {
  parking_tickets: (totals) => {
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
  },
  red_light_locations: (totals) => {
    const totalTickets = totals?.ticketCount ? formatNumber(totals.ticketCount) : null;
    if (totalTickets) {
      return `Ward rankings by red light camera tickets — ${totalTickets} tickets issued.`;
    }
    return 'Ward rankings by red light camera tickets issued.';
  },
  ase_locations: (totals) => {
    const totalTickets = totals?.ticketCount ? formatNumber(totals.ticketCount) : null;
    if (totalTickets) {
      return `Ward rankings by ASE tickets — ${totalTickets} tickets issued.`;
    }
    return 'Ward rankings by automated speed enforcement tickets issued.';
  },
};

function filterValid(items = []) {
  return items.filter((item) => item?.name && item.name !== 'Unknown');
}

function deriveItems(dataset, datasetEntry, initialItems) {
  if (dataset === 'parking_tickets') {
    const context = filterValid(datasetEntry?.topNeighbourhoods || []);
    const fallback = filterValid(initialItems);
    return (context.length > 0 ? context : fallback).slice(0, 10);
  }
  const context = filterValid(datasetEntry?.topGroups?.wards || []);
  const fallback = filterValid(initialItems);
  return (context.length > 0 ? context : fallback).slice(0, 10);
}

export function NeighbourhoodLeaderboard({
  visible = true,
  dataset = 'parking_tickets',
  onNeighbourhoodClick,
  initialItems = [],
  overrideItems = null,
  overrideLoading = false,
  totalsOverride = null,
}) {
  const appData = useAppData();
  const datasetEntry = (appData?.datasets && appData.datasets[dataset]) || null;
  const totalsValue = appData?.totals || null;
  const totals = useMemo(
    () => (
      (totalsOverride && Object.keys(totalsOverride).length > 0)
        ? totalsOverride
        : datasetEntry?.totals || totalsValue || {}
    ),
    [datasetEntry, totalsValue, totalsOverride],
  );
  const [items, setItems] = useState(() => deriveItems(dataset, datasetEntry, initialItems));
  const [loading, setLoading] = useState(() => items.length === 0);

  useEffect(() => {
    if (overrideItems !== null) {
      setLoading(false);
      return;
    }
    const resolved = deriveItems(dataset, datasetEntry, initialItems);
    if (resolved.length > 0) {
      setItems(resolved);
      setLoading(false);
    }
  }, [dataset, datasetEntry, initialItems, overrideItems]);

  useEffect(() => {
    if (overrideItems !== null) {
      return;
    }
    if (dataset !== 'parking_tickets') {
      return;
    }
    if ((datasetEntry?.topNeighbourhoods?.length ?? 0) > 0 || initialItems.length > 0) {
      return;
    }

    fetch(MAP_CONFIG.DATA_PATHS.NEIGHBOURHOOD_STATS)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (!data) {
          throw new Error('Empty response');
        }
        const sorted = Object.entries(data)
          .map(([name, stats]) => ({ name, ...stats }))
          .filter((hood) => hood.name && hood.name !== 'Unknown')
          .sort((a, b) => b.totalFines - a.totalFines)
          .slice(0, 10);
        setItems(sorted);
        setLoading(false);
      })
      .catch((error) => {
        console.error('Failed to load neighbourhood stats:', error);
        setLoading(false);
      });
  }, [dataset, datasetEntry, initialItems, overrideItems]);

  const summaryCopyFactory = SUMMARY_COPY[dataset] || SUMMARY_COPY.parking_tickets;
  const summaryCopy = useMemo(() => summaryCopyFactory(totals), [summaryCopyFactory, totals]);

  if (!visible) {
    return null;
  }

  const handleClick = (name) => {
    if (dataset !== 'parking_tickets') {
      return;
    }
    onNeighbourhoodClick?.(name);
  };

  return (
    <div className="leaderboard neighbourhood-leaderboard">
      <p className="subtitle">{summaryCopy}</p>

      {overrideItems !== null ? (
        overrideLoading && overrideItems.length === 0 ? (
          <div className="loading">Loading...</div>
        ) : (
          <div className="leaderboard-list">
            {overrideItems.map((hood, index) => (
              <div
                key={hood.name || index}
                className={`leaderboard-item ${dataset === 'parking_tickets' ? 'clickable' : 'disabled'}`}
                onClick={() => handleClick(hood.name)}
              >
                <div className="rank">{index + 1}</div>
                <div className="details">
                  <div className="name">{hood.name}</div>
                  <div className="stats">
                    <span className="ticket-count">
                      {formatNumber(hood.ticketCount ?? hood.count ?? 0)} tickets
                    </span>
                    {(() => {
                      const revenueValue = Number(hood.totalRevenue ?? hood.totalFines ?? 0);
                      return Number.isFinite(revenueValue) && revenueValue !== 0
                        ? (
                          <span className="revenue">
                            {formatCurrency(revenueValue)}
                          </span>
                        )
                        : null;
                    })()}
                  </div>
                  {dataset === 'parking_tickets' && hood.topInfraction ? (
                    <div className="top-infraction">
                      Most common: Code {hood.topInfraction}
                    </div>
                  ) : null}
                </div>
              </div>
            ))}
          </div>
        )
      ) : loading ? (
        <div className="loading">Loading...</div>
      ) : (
        <div className="leaderboard-list">
          {items.map((hood, index) => (
            <div
              key={hood.name || index}
              className={`leaderboard-item ${dataset === 'parking_tickets' ? 'clickable' : 'disabled'}`}
              onClick={() => handleClick(hood.name)}
            >
              <div className="rank">{index + 1}</div>
              <div className="details">
                <div className="name">{hood.name}</div>
                <div className="stats">
                  <span className="ticket-count">
                    {formatNumber(hood.count ?? hood.ticketCount ?? 0)} tickets
                  </span>
                  {(() => {
                    const revenueValue = Number(hood.totalFines ?? hood.totalRevenue ?? 0);
                    return Number.isFinite(revenueValue) && revenueValue !== 0
                      ? (
                        <span className="revenue">
                          {formatCurrency(revenueValue)}
                        </span>
                      )
                      : null;
                  })()}
                </div>
                {dataset === 'parking_tickets' && hood.topInfraction ? (
                  <div className="top-infraction">
                    Most common: Code {hood.topInfraction}
                  </div>
                ) : null}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
