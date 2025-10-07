/**
 * InfoPopup - Display detailed ticket information on click
 * Shows address, infraction details, revenue, and violation history
 */
import { useEffect, useRef } from 'react';
import { formatNumber, formatCurrency } from '../lib/dataTransforms';
import '../styles/Controls.css';

export function InfoPopup({ data, position, onClose, variant = 'floating' }) {
  const popupRef = useRef(null);

  useEffect(() => {
    if (!data) return;

    const handleClickOutside = (e) => {
      if (popupRef.current && !popupRef.current.contains(e.target)) {
        onClose?.();
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [data, onClose]);

  useEffect(() => {
    if (!popupRef.current) {
      return;
    }
    popupRef.current.scrollTop = 0;
  }, [data, variant]);

  if (!data) return null;

  const floatingStyle = position ? {
    position: 'fixed',
    left: `${position.x}px`,
    top: `${position.y}px`,
    transform: 'translate(-50%, -50%)',
    zIndex: 10000
  } : {};
  const isFloatingVariant = variant === 'floating';
  const style = isFloatingVariant ? floatingStyle : {};
  const isTicketLocation = Boolean(data?.location);
  const isNeighbourhood = Boolean(!isTicketLocation && data?.name);

  const streetSummary = !isTicketLocation && !isNeighbourhood && typeof data?.street === 'string'
    ? data
    : null;

  const detailCandidate = data?.centrelineDetail ?? data;
  const centrelineDetail = !isTicketLocation && !isNeighbourhood
    ? (detailCandidate?.centrelineId !== undefined || streetSummary ? detailCandidate : null)
    : null;

  const hasStreetContent = Boolean(streetSummary || centrelineDetail);

  const topLocationsSource = centrelineDetail?.topLocations?.length
    ? centrelineDetail.topLocations
    : streetSummary?.topLocations;
  const topLocations = topLocationsSource ? topLocationsSource.slice(0, 5) : [];

  const topInfractionsSource = centrelineDetail?.topInfractions?.length
    ? centrelineDetail.topInfractions
    : streetSummary?.topInfractions;
  const topInfractions = topInfractionsSource ? topInfractionsSource.slice(0, 5) : [];

  const popupTitle = isTicketLocation
    ? `📍 ${data.location}`
    : streetSummary || centrelineDetail
      ? `🛣️ ${streetSummary?.street ?? centrelineDetail?.street ?? 'Street segment'}`
      : isNeighbourhood
        ? data.name
        : 'Details';

  const renderTicketStats = () => {
    const years = data?.years ?? [];
    const months = data?.months ?? [];
    return (
      <div className="popup-stats">
        <div className="popup-stat">
          <strong>Total Tickets:</strong> {formatNumber(data?.count ?? 0)}
        </div>
        <div className="popup-stat">
          <strong>Total Revenue:</strong> {formatCurrency(data?.total_revenue ?? 0)}
        </div>
        {data?.top_infraction && (
          <div className="popup-stat">
            <strong>Most Common Infraction:</strong> Code {data.top_infraction}
          </div>
        )}
        {years.length > 0 && (
          <div className="popup-stat">
            <strong>Years Ticketed:</strong> {years.length} ({years[0]} - {years[years.length - 1]})
          </div>
        )}
        {months.length > 0 && (
          <div className="popup-stat">
            <strong>Active Months:</strong> {months.length} unique months
          </div>
        )}
      </div>
    );
  };

  const renderStreetStats = () => {
    const summary = streetSummary ?? centrelineDetail;
    if (!summary) {
      return null;
    }

    const years = summary.years ?? [];
    const months = summary.months ?? [];
    const centrelineCount = streetSummary?.centrelineIds?.length ?? null;

    return (
      <div className="popup-stats">
        {summary.ticketCount !== undefined && (
          <div className="popup-stat">
            <strong>Total Tickets:</strong> {formatNumber(summary.ticketCount)}
          </div>
        )}
        {summary.totalRevenue !== undefined && (
          <div className="popup-stat">
            <strong>Total Revenue:</strong> {formatCurrency(summary.totalRevenue)}
          </div>
        )}
        {typeof centrelineCount === 'number' && (
          <div className="popup-stat">
            <strong>Segments tracked:</strong> {formatNumber(centrelineCount)}
          </div>
        )}
        {years.length > 0 && (
          <div className="popup-stat">
            <strong>Active Years:</strong> {years[0]} – {years[years.length - 1]} ({years.length})
          </div>
        )}
        {months.length > 0 && (
          <div className="popup-stat">
            <strong>Unique Months:</strong> {months.length}
          </div>
        )}
      </div>
    );
  };

  const renderNeighbourhoodStats = () => (
    <div className="popup-stats">
      {data?.ticketCount !== undefined && (
        <div className="popup-stat">
          <strong>Total Tickets:</strong> {formatNumber(data.ticketCount)}
        </div>
      )}
      {data?.totalFines !== undefined && (
        <div className="popup-stat">
          <strong>Total Fines:</strong> {formatCurrency(data.totalFines)}
        </div>
      )}
      {data?.ticketsPerCapita !== undefined && (
        <div className="popup-stat">
          <strong>Per 1,000 residents:</strong> {data.ticketsPerCapita.toFixed(1)}
        </div>
      )}
    </div>
  );

  const segmentStats = !centrelineDetail || streetSummary === centrelineDetail
    ? null
    : (
      <div className="popup-section">
        <h4>Highlighted Segment</h4>
        <div className="popup-stats">
          {centrelineDetail.centrelineId && (
            <div className="popup-stat">
              <strong>Segment ID:</strong> {centrelineDetail.centrelineId}
            </div>
          )}
          {centrelineDetail.ticketCount !== undefined && (
            <div className="popup-stat">
              <strong>Tickets here:</strong> {formatNumber(centrelineDetail.ticketCount)}
            </div>
          )}
          {centrelineDetail.totalRevenue !== undefined && (
            <div className="popup-stat">
              <strong>Revenue here:</strong> {formatCurrency(centrelineDetail.totalRevenue)}
            </div>
          )}
        </div>
      </div>
    );

  const insights = hasStreetContent ? (
    <>
      {segmentStats}
      {topLocations.length > 0 && (
        <div className="popup-section">
          <h4>Top Hotspots</h4>
          <ul className="popup-list">
            {topLocations.map((location) => {
              const key = `${location.location}-${location.ticketCount}`;
              return (
                <li key={key} className="popup-list-item">
                  <div className="popup-list-label">{location.location}</div>
                  <div className="popup-list-metrics">
                    <span>{formatNumber(location.ticketCount)} tickets</span>
                    {location.totalRevenue !== undefined && (
                      <span>{formatCurrency(location.totalRevenue)}</span>
                    )}
                    {location.topInfraction && (
                      <span>Top infraction: Code {location.topInfraction}</span>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        </div>
      )}
      {topInfractions.length > 0 && (
        <div className="popup-section">
          <h4>Leading Infractions</h4>
          <ul className="popup-list">
            {topInfractions.map((infraction) => (
              <li key={infraction.code} className="popup-list-item">
                <div className="popup-list-label">Code {infraction.code}</div>
                <div className="popup-list-metrics">
                  <span>{formatNumber(infraction.count)} tickets</span>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
    </>
  ) : null;

  const classNames = ['info-popup', `popup-variant-${variant}`];
  if (isTicketLocation) {
    classNames.push('ticket-popup');
  }
  if (hasStreetContent) {
    classNames.push('street-popup');
  }

  if (!isTicketLocation && !isNeighbourhood && !hasStreetContent) {
    return null;
  }

  const statsContent = isTicketLocation
    ? renderTicketStats()
    : hasStreetContent
      ? renderStreetStats()
      : renderNeighbourhoodStats();

  return (
    <div ref={popupRef} className={classNames.join(' ')} style={style}>
      <button className="close-btn" onClick={onClose}>×</button>
      <h3>{popupTitle}</h3>
      {statsContent}
      {insights}
    </div>
  );
}
