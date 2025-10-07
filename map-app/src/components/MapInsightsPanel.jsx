import { StatsSummary } from './StatsSummary.jsx';
import { ViewportInsights } from './ViewportInsights.jsx';
import '../styles/MapInsightsPanel.css';

export function MapInsightsPanel({ viewportSummary, fallbackTopStreets }) {
  return (
    <aside className="map-insights-panel">
      <StatsSummary viewportSummary={viewportSummary} variant="compact" />
      <ViewportInsights
        summary={viewportSummary}
        fallbackTopStreets={fallbackTopStreets}
        variant="compact"
      />
    </aside>
  );
}
