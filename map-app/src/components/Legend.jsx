/**
 * Legend - Map attribution and legend
 * Single responsibility: display data sources and color scales
 */
import { useMemo, useState } from 'react';
import { STYLE_CONSTANTS } from '../lib/mapSources';
import { DATASET_LEGEND_CONTENT } from '../lib/legendContent.js';
import '../styles/Controls.css';

export function Legend({ visible = true, dataset = 'parking_tickets' }) {
  const [isExpanded, setIsExpanded] = useState(false);
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

  const datasetLegend = DATASET_LEGEND_CONTENT[dataset] || DATASET_LEGEND_CONTENT.parking_tickets;
  const showGlowSection = dataset === 'parking_tickets';

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
        <svg
          width="20"
          height="20"
          viewBox="0 0 20 20"
          fill="none"
          aria-hidden="true"
          suppressHydrationWarning
        >
          <circle cx="10" cy="10" r="9" stroke="currentColor" strokeWidth="1.5" suppressHydrationWarning />
          <path d="M10 14V10M10 6H10.01" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" suppressHydrationWarning />
        </svg>
        {isExpanded && (
          <span className="legend-attribution">
            MapLibre | © MapTiler © OpenStreetMap contributors
          </span>
        )}
      </button>

      <a
        className="legend-support"
        href="https://ko-fi.com/Z8Z51MBSO5"
        target="_blank"
        rel="noopener noreferrer"
        aria-label="Buy Moe a coffee on Ko-fi"
      >
        <img
          src="https://storage.ko-fi.com/cdn/kofi3.png?v=6"
          alt="Buy Me a Coffee"
          height="36"
        />
      </a>

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
            <div className="legend-title">{datasetLegend.title}</div>
            {datasetLegend.items.map((item) => (
              <div className="legend-item" key={item.label}>
                <div
                  className="legend-color circle"
                  style={{
                    backgroundColor: item.color,
                    borderColor: item.strokeColor || 'rgba(255, 255, 255, 0.2)',
                    borderWidth: item.strokeColor ? 2 : 1,
                    borderStyle: 'solid',
                  }}
                />
                <span>{item.label}</span>
              </div>
            ))}
            {datasetLegend.note ? (
              <p className="legend-note">{datasetLegend.note}</p>
            ) : null}
          </div>

          {showGlowSection && glowGradient && (
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
