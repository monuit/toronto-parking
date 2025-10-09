import { formatCurrency, formatNumber } from '../lib/dataTransforms';

export function WardHoverPopup({ data, dataset }) {
  if (!data) {
    return null;
  }

  const title = data.wardName || (data.wardCode ? `Ward ${data.wardCode}` : 'Ward');
  const locationCount = Number.isFinite(data.locationCount) ? data.locationCount : null;
  const ticketCount = Number.isFinite(data.ticketCount) ? data.ticketCount : null;
  const totalRevenue = Number.isFinite(data.totalRevenue) ? data.totalRevenue : null;
  const aseTicketCount = Number.isFinite(data.aseTicketCount) ? data.aseTicketCount : null;
  const redLightTicketCount = Number.isFinite(data.redLightTicketCount) ? data.redLightTicketCount : null;
  const aseTotalRevenue = Number.isFinite(data.aseTotalRevenue) ? data.aseTotalRevenue : null;
  const redLightTotalRevenue = Number.isFinite(data.redLightTotalRevenue) ? data.redLightTotalRevenue : null;
  const showCombinedBreakdown = dataset === 'cameras_combined'
    && (
      aseTicketCount !== null
      || redLightTicketCount !== null
      || aseTotalRevenue !== null
      || redLightTotalRevenue !== null
    );
  const datasetLabel = (() => {
    if (!dataset) {
      return 'Ward view';
    }
    if (dataset === 'red_light_locations') {
      return 'Red light cameras';
    }
    if (dataset === 'ase_locations') {
      return 'Automated speed enforcement';
    }
    if (dataset === 'cameras_combined') {
      return 'Traffic enforcement (combined)';
    }
    return dataset;
  })();

  return (
    <div className="ward-hover-popup" role="presentation" aria-live="polite">
      <div className="ward-hover-header">
        <span className="ward-hover-dataset">{datasetLabel}</span>
        <h4>{title}</h4>
      </div>
      <div className="ward-hover-body">
        {ticketCount !== null ? (
          <div className="ward-hover-stat">
            <span className="label">Tickets</span>
            <span className="value">{formatNumber(ticketCount)}</span>
          </div>
        ) : null}
        {totalRevenue !== null ? (
          <div className="ward-hover-stat">
            <span className="label">Revenue</span>
            <span className="value">{formatCurrency(totalRevenue)}</span>
          </div>
        ) : null}
        {locationCount !== null ? (
          <div className="ward-hover-stat">
            <span className="label">Locations</span>
            <span className="value">{formatNumber(locationCount)}</span>
          </div>
        ) : null}
        {showCombinedBreakdown ? (
          <>
            {(aseTicketCount !== null || aseTotalRevenue !== null) ? (
              <div className="ward-hover-stat ward-hover-stat--nested">
                <span className="ward-hover-source">Automated speed enforcement</span>
                {aseTicketCount !== null ? (
                  <div className="ward-hover-metric">
                    <span className="ward-hover-metric-label">Tickets</span>
                    <span className="ward-hover-metric-value">{formatNumber(aseTicketCount)}</span>
                  </div>
                ) : null}
                {aseTotalRevenue !== null ? (
                  <div className="ward-hover-metric">
                    <span className="ward-hover-metric-label">Fines</span>
                    <span className="ward-hover-metric-value">{formatCurrency(aseTotalRevenue)}</span>
                  </div>
                ) : null}
              </div>
            ) : null}
            {(redLightTicketCount !== null || redLightTotalRevenue !== null) ? (
              <div className="ward-hover-stat ward-hover-stat--nested">
                <span className="ward-hover-source">Red light cameras</span>
                {redLightTicketCount !== null ? (
                  <div className="ward-hover-metric">
                    <span className="ward-hover-metric-label">Tickets</span>
                    <span className="ward-hover-metric-value">{formatNumber(redLightTicketCount)}</span>
                  </div>
                ) : null}
                {redLightTotalRevenue !== null ? (
                  <div className="ward-hover-metric">
                    <span className="ward-hover-metric-label">Fines</span>
                    <span className="ward-hover-metric-value">{formatCurrency(redLightTotalRevenue)}</span>
                  </div>
                ) : null}
              </div>
            ) : null}
          </>
        ) : null}
      </div>
    </div>
  );
}

export default WardHoverPopup;
