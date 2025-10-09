import { useWardData } from '../context/WardDataContext.jsx';
import '../styles/Controls.css';

const SUPPORTED_DATASETS = new Set(['red_light_locations', 'ase_locations']);

function getDatasetLabel(dataset) {
  if (dataset === 'red_light_locations') {
    return 'Red light only';
  }
  if (dataset === 'ase_locations') {
    return 'ASE only';
  }
  return 'Dataset';
}

export function WardModeToggle({
  dataset,
  viewMode = 'detail',
  wardDataset,
  onViewModeChange,
  onWardDatasetChange,
}) {
  const { preloadDataset } = useWardData();
  if (!SUPPORTED_DATASETS.has(dataset)) {
    return null;
  }

  const activeWardDataset = wardDataset || dataset;
  const handlePreload = (targetDataset) => () => {
    preloadDataset?.(targetDataset);
  };

  return (
    <div className="controls-row ward-mode-toggle">
      <div className="toggle-group">
        <button
          type="button"
          className={viewMode === 'detail' ? 'active' : ''}
          onClick={() => onViewModeChange?.('detail')}
          onMouseEnter={handlePreload(dataset)}
          onFocus={handlePreload(dataset)}
        >
          Details view
        </button>
        <button
          type="button"
          className={viewMode === 'ward' ? 'active' : ''}
          onClick={() => onViewModeChange?.('ward')}
          onMouseEnter={handlePreload(activeWardDataset)}
          onFocus={handlePreload(activeWardDataset)}
        >
          Ward view
        </button>
      </div>

      {viewMode === 'ward' ? (
        <div className="toggle-group ward-dataset-group">
          <button
            type="button"
            className={activeWardDataset === dataset ? 'active' : ''}
            onClick={() => onWardDatasetChange?.(dataset)}
            onMouseEnter={handlePreload(dataset)}
            onFocus={handlePreload(dataset)}
          >
            {getDatasetLabel(dataset)}
          </button>
          <button
            type="button"
            className={activeWardDataset === 'cameras_combined' ? 'active' : ''}
            onClick={() => onWardDatasetChange?.('cameras_combined')}
            onMouseEnter={handlePreload('cameras_combined')}
            onFocus={handlePreload('cameras_combined')}
          >
            Combined
          </button>
        </div>
      ) : null}
    </div>
  );
}

export default WardModeToggle;
