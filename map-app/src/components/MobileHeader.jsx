import PropTypes from 'prop-types';
import { DatasetToggle } from './DatasetToggle.jsx';

export function MobileHeader({
  dataset,
  onDatasetChange,
  onDrawerToggle,
  isDrawerOpen,
  children,
  onLegendToggle,
  onInsightsToggle,
}) {
  return (
    <header className="mobile-header" role="banner">
      <div className="mobile-header__row">
        <button
          type="button"
          className="mobile-header__drawer-button"
          onClick={onDrawerToggle}
          aria-expanded={isDrawerOpen}
        >
          {isDrawerOpen ? 'Close panels' : 'Open panels'}
        </button>
        <div className="mobile-header__toggle">
          <DatasetToggle value={dataset} onChange={onDatasetChange} />
        </div>
        <div className="mobile-header__actions">
          {children}
        </div>
      </div>
      <div className="mobile-header__fab-row">
        <button type="button" className="mobile-fab mobile-fab--small" onClick={onInsightsToggle}>
          Insights
        </button>
        <button type="button" className="mobile-fab mobile-fab--small" onClick={onLegendToggle}>
          Legend
        </button>
      </div>
    </header>
  );
}

MobileHeader.propTypes = {
  dataset: PropTypes.string.isRequired,
  onDatasetChange: PropTypes.func.isRequired,
  onDrawerToggle: PropTypes.func.isRequired,
  isDrawerOpen: PropTypes.bool.isRequired,
  children: PropTypes.node,
  onLegendToggle: PropTypes.func,
  onInsightsToggle: PropTypes.func,
};

MobileHeader.defaultProps = {
  children: null,
  onLegendToggle: () => {},
  onInsightsToggle: () => {},
};
