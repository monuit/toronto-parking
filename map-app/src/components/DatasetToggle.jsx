import PropTypes from 'prop-types';
import './DatasetToggle.css';

const DATASET_LABELS = {
  parking_tickets: 'TO Parking',
  red_light_locations: 'Red Light',
  ase_locations: 'ASE',
};

export function DatasetToggle({ value, onChange }) {
  return (
    <div className="dataset-toggle" role="tablist" aria-label="Dataset selector">
      {Object.entries(DATASET_LABELS).map(([dataset, label]) => {
        const isActive = dataset === value;
        return (
          <button
            key={dataset}
            type="button"
            className={`dataset-toggle__button ${isActive ? 'dataset-toggle__button--active' : ''}`}
            onClick={() => onChange(dataset)}
            role="tab"
            aria-selected={isActive}
          >
            {label}
          </button>
        );
      })}
    </div>
  );
}

DatasetToggle.propTypes = {
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
};
