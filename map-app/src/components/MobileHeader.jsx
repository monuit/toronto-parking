import PropTypes from 'prop-types';
import { DatasetToggle } from './DatasetToggle.jsx';
import { MiniInsightsPanel } from './MiniInsightsPanel.jsx';

export function MobileHeader({
  dataset,
  displayDataset,
  onDatasetChange,
  totals,
  year,
  onInsightsOpen,
  children,
  searchSlot,
}) {
  const insightsDataset = displayDataset || dataset;
  return (
    <div className="mobile-shell" role="region" aria-label="Map controls">
      <div className="mobile-shell__insights">
        <MiniInsightsPanel
          dataset={insightsDataset}
          totals={totals}
          year={year}
          onExpand={onInsightsOpen}
        />
      </div>

      <div className="mobile-shell__controls">
        <div className="mobile-shell__dataset">
          <DatasetToggle value={dataset} onChange={onDatasetChange} />
        </div>
        {children ? (
          <div className="mobile-shell__secondary">
            {children}
          </div>
        ) : null}
      </div>

      {searchSlot ? (
        <div className="mobile-shell__search">
          {searchSlot}
        </div>
      ) : null}
    </div>
  );
}

MobileHeader.propTypes = {
  dataset: PropTypes.string.isRequired,
  displayDataset: PropTypes.string,
  onDatasetChange: PropTypes.func.isRequired,
  totals: PropTypes.shape({
    ticketCount: PropTypes.number,
    totalRevenue: PropTypes.number,
    locationCount: PropTypes.number,
    featureCount: PropTypes.number,
  }),
  year: PropTypes.number,
  onInsightsOpen: PropTypes.func,
  children: PropTypes.node,
  searchSlot: PropTypes.node,
};

MobileHeader.defaultProps = {
  displayDataset: null,
  totals: null,
  year: null,
  onInsightsOpen: () => {},
  children: null,
  searchSlot: null,
};
