import PropTypes from 'prop-types';
import { formatCurrency, formatNumber } from '../lib/dataTransforms.js';

const DATASET_LABELS = {
  parking_tickets: 'Parking tickets',
  red_light_locations: 'Red light charges',
  ase_locations: 'ASE charges',
  cameras_combined: 'Traffic cameras',
};

export function MiniInsightsPanel({ dataset, totals, year, onExpand }) {
  const label = DATASET_LABELS[dataset] || 'Dataset summary';
  const ticketCount = totals ? formatNumber(Number(totals.ticketCount ?? totals.featureCount ?? 0)) : '—';
  const revenueTotal = totals ? formatCurrency(Number(totals.totalRevenue ?? 0)) : '—';
  const locationCount = totals ? formatNumber(Number(totals.locationCount ?? totals.featureCount ?? 0)) : null;

  return (
    <button
      type="button"
      className="mini-insights"
      onClick={onExpand}
      aria-label="View detailed insights"
    >
      <div className="mini-insights__header">
        <span className="mini-insights__label">{label}</span>
        {Number.isFinite(year) ? (
          <span className="mini-insights__pill">{year}</span>
        ) : null}
      </div>
      <div className="mini-insights__metrics">
        <span className="mini-insights__metric">
          <strong>{ticketCount}</strong>
          <span aria-hidden="true"> tickets</span>
        </span>
        <span className="mini-insights__separator" aria-hidden="true">•</span>
        <span className="mini-insights__metric">
          <strong>{revenueTotal}</strong>
          <span aria-hidden="true"> total fines</span>
        </span>
      </div>
      {locationCount ? (
        <div className="mini-insights__meta">{locationCount} locations tracked</div>
      ) : null}
    </button>
  );
}

MiniInsightsPanel.propTypes = {
  dataset: PropTypes.string.isRequired,
  totals: PropTypes.shape({
    ticketCount: PropTypes.number,
    featureCount: PropTypes.number,
    totalRevenue: PropTypes.number,
    locationCount: PropTypes.number,
  }),
  year: PropTypes.number,
  onExpand: PropTypes.func,
};

MiniInsightsPanel.defaultProps = {
  totals: null,
  year: null,
  onExpand: () => {},
};
