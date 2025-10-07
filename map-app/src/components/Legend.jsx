/**
 * Legend - Map attribution and legend
 * Single responsibility: display data sources and color scales
 */
import { useMemo, useState } from 'react';
import { STYLE_CONSTANTS } from '../lib/mapSources';
import '../styles/Controls.css';

const CLUSTER_STEPS = [
  { label: '1 – 99 tickets', size: 'sm' },
  { label: '100 – 749 tickets', size: 'md' },
  { label: '750 – 4,999 tickets', size: 'lg' },
  { label: '5,000+ tickets', size: 'xl' },
];

export function Legend({ visible = true }) {
  const [isExpanded, setIsExpanded] = useState(false);
  const clusterSteps = useMemo(() => CLUSTER_STEPS, []);
  const glowStops = useMemo(() => STYLE_CONSTANTS.CITY_GLOW_STOPS, []);
  const glowGradient = useMemo(() => {
    if (!glowStops.length) {
      return null;
    }
    const maxValue = glowStops[glowStops.length - 1].value;
    const gradientStops = glowStops
      .map(({ color, value }) => {
        const percent = Math.round((value / maxValue) * 1000) / 10;
        return `${color} ${percent}%`;
      })
      .join(', ');
    return `linear-gradient(90deg, ${glowStops[0].color} 0%, ${gradientStops})`;
  }, [glowStops]);
  const peakGlowColor = useMemo(
    () => glowStops[glowStops.length - 1]?.color ?? '#ffffff',
    [glowStops],
  );

  if (!visible) return null;

  return (
    <div className={`legend ${isExpanded ? 'expanded' : 'collapsed'}`}>
      {isExpanded && (
        <div className="legend-traffic-lights">
          <button
            className="traffic-light red"
            onClick={() => setIsExpanded(false)}
            aria-label="Close legend"
          />
          <button className="traffic-light yellow" disabled aria-label="Minimize" />
          <button className="traffic-light green" disabled aria-label="Maximize" />
        </div>
      )}

      <button
        className="legend-toggle"
        onClick={() => setIsExpanded(!isExpanded)}
        aria-label="Toggle legend"
      >
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <circle cx="10" cy="10" r="9" stroke="currentColor" strokeWidth="1.5"/>
          <path d="M10 14V10M10 6H10.01" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
        </svg>
        {isExpanded && (
          <span className="legend-attribution">
            MapLibre | © MapTiler © OpenStreetMap contributors
          </span>
        )}
      </button>

      {isExpanded && (
        <div className="legend-content">
          <h4>Legend</h4>
          <p className="legend-intro">
            Ticket counts are aggregated by location. The dollar amounts represent total fines in Canadian dollars (CAD).
          </p>

          <div className="legend-section">
            <div className="legend-title">Neighbourhoods</div>
            <div className="legend-item">
              <div
                className="legend-color"
                style={{ backgroundColor: STYLE_CONSTANTS.COLORS.NEIGHBOURHOOD_FILL }}
              />
              <span>Boundary</span>
            </div>
          </div>

          <div className="legend-section">
            <div className="legend-title">Parking Tickets</div>
            <div className="legend-item">
              <div
                className="legend-color circle"
                style={{ backgroundColor: STYLE_CONSTANTS.COLORS.TICKET_CLUSTER }}
              />
              <span>Cluster (grouped tickets)</span>
            </div>
            <div className="legend-item">
              <div
                className="legend-color circle"
                style={{ backgroundColor: STYLE_CONSTANTS.COLORS.TICKET_POINT }}
              />
              <span>Individual ticket</span>
            </div>
            <ul className="legend-clusters">
              {clusterSteps.map((step) => (
                <li key={step.label} className="legend-cluster-item">
                  <span className={`legend-dot legend-dot--${step.size}`} />
                  <span>{step.label}</span>
                </li>
              ))}
            </ul>
            <p className="legend-note">
              Clusters compress nearby tickets so the map stays readable. Zoom in past neighbourhood level to reveal individual streets and addresses.
            </p>
          </div>

          {glowGradient && (
            <div className="legend-section">
              <div className="legend-title">Street glow (tickets / 100m)</div>
              <div
                className="legend-gradient-bar"
                style={{ backgroundImage: glowGradient, color: peakGlowColor }}
                role="presentation"
              />
              <div className="legend-gradient-labels">
                {glowStops.map((stop) => (
                  <span key={stop.value}>{stop.label}</span>
                ))}
              </div>
            </div>
          )}

          <div className="attribution">
            <small>
              Data: City of Toronto Open Data Portal<br/>
              Map: © OpenStreetMap contributors
            </small>
            <span className="legend-mobile-hint">Tip: tap the info button to open this legend on mobile.</span>
          </div>
        </div>
      )}
    </div>
  );
}
