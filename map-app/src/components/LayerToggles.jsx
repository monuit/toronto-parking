/**
 * LayerToggles - UI controls for showing/hiding map layers
 * Single responsibility: manage layer visibility state
 */
import { useEffect, useMemo, useState } from 'react';
import '../styles/Controls.css';

export function LayerToggles({ onToggle, layers: parentLayers }) {
  const defaultState = useMemo(() => ({
    cityGlow: true,
    neighbourhoods: false,
    tickets: false
  }), []);

  const [layers, setLayers] = useState(defaultState);

  // Sync with parent layers state
  useEffect(() => {
    if (parentLayers) {
      setLayers({
        cityGlow: parentLayers.cityGlow ?? parentLayers.heatmap ?? defaultState.cityGlow,
        neighbourhoods: parentLayers.neighbourhoods ?? defaultState.neighbourhoods,
        tickets: parentLayers.tickets ?? defaultState.tickets
      });
    }
  }, [defaultState, parentLayers]);

  const handleToggle = (layerName) => {
    const newLayers = {
      ...layers,
      [layerName]: !layers[layerName]
    };
    setLayers(newLayers);

    if (onToggle) {
      onToggle(layerName, newLayers[layerName]);
    }
  };

  return (
    <div className="layer-toggles glass-panel">
      <h3 className="controls-title">Layers</h3>
      <div className="toggle-list">
        <label className="toggle-item">
          <input
            type="checkbox"
            checked={layers.cityGlow}
            onChange={() => handleToggle('cityGlow')}
          />
          <span className="toggle-icon">🌆</span>
          <span>City glow</span>
        </label>

        <label className="toggle-item">
          <input
            type="checkbox"
            checked={layers.neighbourhoods}
            onChange={() => handleToggle('neighbourhoods')}
          />
          <span className="toggle-icon">🗺️</span>
          <span>Neighbourhoods</span>
        </label>

        <label className="toggle-item">
          <input
            type="checkbox"
            checked={layers.tickets}
            onChange={() => handleToggle('tickets')}
          />
          <span className="toggle-icon">📍</span>
          <span>Tickets</span>
        </label>
      </div>
    </div>
  );
}
