import { formatCurrency, formatNumber } from '../lib/dataTransforms';
import { useAppData } from '../context/AppDataContext.jsx';
import '../styles/StatsSummary.css';

export function StatsSummary({
  viewportSummary,
  variant = 'default',
  showTotals = true,
  showViewport = true,
  title = 'Toronto Parking Tickets',
  viewportTitle = 'Current view',
}) {
  const { totals } = useAppData();
  const totalTickets = formatNumber(totals.ticketCount || 0);
  const totalRevenue = formatCurrency(totals.totalRevenue || 0);
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
          <h2>{title}</h2>
          <div className="totals-grid">
            <div>
              <span className="label">Tickets recorded</span>
              <span className="value">{totalTickets}</span>
            </div>
            <div>
              <span className="label">Total fines</span>
              <span className="value">{totalRevenue}</span>
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
