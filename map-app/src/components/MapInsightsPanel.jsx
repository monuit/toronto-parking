import { StatsSummary } from './StatsSummary.jsx';
import { ViewportInsights } from './ViewportInsights.jsx';
import '../styles/MapInsightsPanel.css';

export function MapInsightsPanel({ viewportSummary, fallbackTopStreets, dataset = 'parking_tickets', totalsOverride = null }) {
  return (
    <aside className="map-insights-panel">
      <StatsSummary
        viewportSummary={viewportSummary}
        variant="compact"
        dataset={dataset}
        totalsOverride={totalsOverride}
      />
      <ViewportInsights
        summary={viewportSummary}
        fallbackTopStreets={dataset === 'parking_tickets' ? fallbackTopStreets : []}
        variant="compact"
      />
    </aside>
  );
}
