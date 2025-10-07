import { formatCurrency, formatNumber } from '../lib/dataTransforms';
import '../styles/ViewportInsights.css';

export function ViewportInsights({ summary, fallbackTopStreets, variant = 'default' }) {
  const topList = summary?.topStreets?.length ? summary.topStreets : fallbackTopStreets?.slice(0, 5);
  const classes = ['viewport-insights'];

  if (variant === 'compact') {
    classes.push('viewport-insights--compact');
  }

  return (
    <div className={classes.join(' ')}>
      <h3>Top streets in view</h3>

      {summary?.zoomRestricted ? (
        <p className="hint">Zoom to street level to see the top 5 streets for this area.</p>
      ) : null}

      {!summary?.zoomRestricted && summary?.topStreets?.length === 0 ? (
        <p className="hint">Move the map to highlight an area with recorded tickets.</p>
      ) : null}

      {topList && topList.length > 0 ? (
        <ol className="street-list">
          {topList.slice(0, 5).map((street, index) => (
            <li key={street.name || index}>
              <div className="primary-row">
                <span className="rank">#{index + 1}</span>
                <span className="name">{street.name}</span>
              </div>
              <div className="secondary-row">
                <span>{formatNumber(street.ticketCount || 0)} tickets</span>
                <span>{formatCurrency(street.totalRevenue || 0)}</span>
              </div>
            </li>
          ))}
        </ol>
      ) : null}
    </div>
  );
}
