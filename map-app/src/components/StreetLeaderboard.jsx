/**
 * StreetLeaderboard - Display top streets by ticket count
 * Single responsibility: render street statistics in ranked list
 */
import { useEffect, useMemo, useState } from 'react';
import { formatNumber, formatCurrency } from '../lib/dataTransforms';
import { useAppData } from '../context/AppDataContext.jsx';
import '../styles/Leaderboard.css';

const SUMMARY_COPY = {
  parking_tickets: (totals) => {
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
  },
  red_light_locations: (totals) => {
    const ticketCount = totals?.ticketCount ? formatNumber(totals.ticketCount) : null;
    const locations = totals?.locationCount ? formatNumber(totals.locationCount) : null;
    if (ticketCount && locations) {
      return `Top camera intersections by tickets issued — ${ticketCount} tickets across ${locations} sites.`;
    }
    return 'Top red light camera intersections by tickets issued.';
  },
  ase_locations: (totals) => {
    const ticketCount = totals?.ticketCount ? formatNumber(totals.ticketCount) : null;
    const locations = totals?.locationCount ? formatNumber(totals.locationCount) : null;
    if (ticketCount && locations) {
      return `Top ASE camera sites — ${ticketCount} tickets across ${locations} locations.`;
    }
    return 'Top automated speed enforcement camera sites by tickets issued.';
  },
};

function deriveDatasetItems(dataset, contextEntry, initialItems) {
  if (dataset === 'parking_tickets') {
    const contextList = Array.isArray(contextEntry?.topStreets) ? contextEntry.topStreets : [];
    return contextList.length > 0 ? contextList.slice(0, 10) : initialItems.slice(0, 10);
  }
  const contextList = Array.isArray(contextEntry?.topLocations) ? contextEntry.topLocations : [];
  return contextList.length > 0 ? contextList.slice(0, 10) : initialItems.slice(0, 10);
}

export function StreetLeaderboard({
  visible = true,
  dataset = 'parking_tickets',
  initialItems = [],
  onStreetSelect,
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
  const [items, setItems] = useState(() => deriveDatasetItems(dataset, datasetEntry, initialItems));
  const [loading, setLoading] = useState(() => items.length === 0);

  useEffect(() => {
    if (overrideItems !== null) {
      setLoading(false);
      return;
    }
    const resolved = deriveDatasetItems(dataset, datasetEntry, initialItems);
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
    if ((datasetEntry?.topStreets?.length ?? 0) > 0 || initialItems.length > 0) {
      return;
    }

    fetch('/data/street_stats.json')
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (!data) {
          throw new Error('Empty response');
        }
        const sorted = Object.entries(data)
          .map(([name, stats]) => ({ name, ...stats }))
          .sort((a, b) => b.totalRevenue - a.totalRevenue)
          .slice(0, 10);
        setItems(sorted);
        setLoading(false);
      })
      .catch((error) => {
        console.error('Failed to load street stats:', error);
        setLoading(false);
      });
  }, [dataset, datasetEntry, initialItems, overrideItems]);

  const summaryCopyFactory = SUMMARY_COPY[dataset] || SUMMARY_COPY.parking_tickets;
  const summaryCopy = useMemo(() => summaryCopyFactory(totals), [summaryCopyFactory, totals]);

  const resolvedItems = overrideItems ?? items;
  const resolvedLoading = overrideItems !== null
    ? Boolean(overrideLoading && (overrideItems?.length === 0))
    : loading;

  if (!visible) {
    return null;
  }

  const handleSelect = (item) => {
    if (typeof onStreetSelect === 'function') {
      onStreetSelect(item);
    }
  };

  return (
    <div className="leaderboard street-leaderboard">
      <p className="subtitle">{summaryCopy}</p>

      {resolvedLoading ? (
        <div className="loading">Loading...</div>
      ) : (
        <div className="leaderboard-list">
          {resolvedItems.map((entry, index) => {
            const name = entry.sampleLocation || entry.address || entry.name;
            const revenueCandidate = Number(entry.totalRevenue ?? entry.total_revenue ?? 0);
            const revenue = Number.isFinite(revenueCandidate) && revenueCandidate !== 0
              ? revenueCandidate
              : (Number.isFinite(Number(entry.totalRevenue)) ? Number(entry.totalRevenue) : null);
            const ticketCountValue = Number(entry.ticketCount ?? entry.count ?? 0);
            const interactive = typeof onStreetSelect === 'function';
            return (
              <button
                key={entry.id || name || index}
                type="button"
                className={`leaderboard-item ${interactive ? '' : 'disabled'}`}
                onClick={() => handleSelect(entry)}
                disabled={!interactive}
              >
                <div className="rank">{index + 1}</div>
                <div className="details">
                  <div className="name">{name}</div>
                  <div className="stats">
                    <span className="ticket-count">
                        {formatNumber(ticketCountValue)} tickets
                    </span>
                    {revenue !== null ? (
                      <span className="revenue">{formatCurrency(revenue)}</span>
                    ) : null}
                  </div>
                  {dataset === 'parking_tickets' && entry.topInfraction ? (
                    <div className="top-infraction">
                      Most common: Code {parseFloat(entry.topInfraction).toString()}
                    </div>
                  ) : null}
                  {dataset !== 'parking_tickets' && (entry.ward || entry.status || entry.policeDivision) ? (
                    <div className="top-infraction">
                      {[entry.status, entry.ward, entry.policeDivision].filter(Boolean).join(' • ')}
                    </div>
                  ) : null}
                </div>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
