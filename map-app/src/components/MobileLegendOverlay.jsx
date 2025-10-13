import PropTypes from 'prop-types';
import { DATASET_LEGEND_CONTENT } from '../lib/legendContent.js';

export function MobileLegendOverlay({ dataset, expanded, onToggle }) {
  const legend = DATASET_LEGEND_CONTENT[dataset] || DATASET_LEGEND_CONTENT.parking_tickets;

  return (
    <div className={`mobile-legend ${expanded ? 'mobile-legend--open' : ''}`}>
      <button
        type="button"
        className="mobile-legend__button"
        onClick={onToggle}
        aria-expanded={expanded}
        aria-controls="mobile-legend-panel"
      >
        i
      </button>
      {expanded ? (
        <div className="mobile-legend__panel" id="mobile-legend-panel">
          <h3>{legend.title}</h3>
          <ul className="mobile-legend__items">
            {legend.items.map((item) => (
              <li key={item.label} className="mobile-legend__item">
                <span
                  className="mobile-legend__swatch"
                  style={{
                    backgroundColor: item.color,
                    borderColor: item.strokeColor || 'rgba(255, 255, 255, 0.4)',
                  }}
                />
                <span>{item.label}</span>
              </li>
            ))}
          </ul>
          {legend.note ? <p className="mobile-legend__note">{legend.note}</p> : null}
        </div>
      ) : null}
    </div>
  );
}

MobileLegendOverlay.propTypes = {
  dataset: PropTypes.string.isRequired,
  expanded: PropTypes.bool,
  onToggle: PropTypes.func,
};

MobileLegendOverlay.defaultProps = {
  expanded: false,
  onToggle: () => {},
};
