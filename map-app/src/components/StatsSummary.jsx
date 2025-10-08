import { formatCurrency, formatNumber } from '../lib/dataTransforms';
import { useAppData } from '../context/AppDataContext.jsx';
import '../styles/StatsSummary.css';

const DATASET_TITLES = {
  parking_tickets: 'Toronto Parking Tickets',
  red_light_locations: 'Red Light Camera Charges',
  ase_locations: 'Automated Speed Enforcement Charges',
};

export function StatsSummary({
  viewportSummary,
  variant = 'default',
  showTotals = true,
  showViewport = true,
  dataset = 'parking_tickets',
  totalsOverride = null,
  title,
  viewportTitle = 'Current view',
}) {
  const { totals: contextTotals } = useAppData();
  const totalsSource = (totalsOverride && Object.keys(totalsOverride).length > 0)
    ? totalsOverride
    : contextTotals || {};
  const toNumber = (value, fallback = 0) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : fallback;
  };
  const isParkingDataset = dataset === 'parking_tickets';
  const locationCountValue = toNumber(totalsSource.featureCount, 0);
  const ticketsCountValue = toNumber(
    totalsSource.ticketCount ?? (isParkingDataset ? totalsSource.featureCount : undefined),
    0,
  );
  const totalRevenueValue = toNumber(totalsSource.totalRevenue, 0);
  const heading = title || DATASET_TITLES[dataset] || 'Dataset overview';
  const hasViewportData = !viewportSummary?.zoomRestricted && typeof viewportSummary?.visibleCount === 'number';
  const classes = ['stats-summary'];

  if (variant === 'compact') {
    classes.push('stats-summary--compact');
  }

  if (!showTotals) {
    classes.push('stats-summary--viewport-only');
  }

  return (
    <div className={classes.join(' ')}>
      {showTotals ? (
        <div className="totals">
          <h2>{heading}</h2>
          <div className="totals-grid">
            {isParkingDataset ? (
              <div>
                <span className="label">Tickets recorded</span>
                <span className="value">{formatNumber(ticketsCountValue)}</span>
              </div>
            ) : (
              <>
                <div>
                  <span className="label">Locations tracked</span>
                  <span className="value">{formatNumber(locationCountValue)}</span>
                </div>
                <div>
                  <span className="label">Tickets issued</span>
                  <span className="value">{formatNumber(ticketsCountValue)}</span>
                </div>
              </>
            )}
            <div>
              <span className="label">Total fines</span>
              <span className="value">{formatCurrency(totalRevenueValue)}</span>
            </div>
          </div>
        </div>
      ) : null}

      {showViewport ? (
        <div className="viewport">
          <h3>{viewportTitle}</h3>
          {viewportSummary?.zoomRestricted && (
            <p className="hint">Zoom in to see street-level insights and the top 5 streets for this area.</p>
          )}

          {hasViewportData ? (
            <div className="totals-grid">
              <div>
                <span className="label">Tickets in view</span>
                <span className="value">{formatNumber(viewportSummary.visibleCount)}</span>
              </div>
              <div>
                <span className="label">Fines in view</span>
                <span className="value">{formatCurrency(viewportSummary.visibleRevenue || 0)}</span>
              </div>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
