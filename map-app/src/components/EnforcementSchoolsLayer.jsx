/**
 * EnforcementSchoolsLayer - Combined map layers for schools + ASE + red light cameras
 * Single responsibility: render schools and enforcement cameras with status-based colors
 */
import { useEffect, useState } from 'react';
import { Source, Layer } from 'react-map-gl/maplibre';
import { MAP_CONFIG, STYLE_CONSTANTS } from '../lib/mapSources';

const API_BASE = import.meta.env?.VITE_API_BASE_URL || '';
const ENFORCEMENT_SCHOOLS_URL = `${API_BASE}/api/enforcement-schools`;

export function EnforcementSchoolsLayer({
  map,
  visible = true,
  showSchools = true,
  showASE = true,
  showRedLight = true,
  onError = () => {},
}) {
  const [data, setData] = useState(null);

  // MARK: Fetch data from API
  useEffect(() => {
    if (!visible || !map) {
      return;
    }

    const fetchData = async () => {
      try {
        // Build type filter
        const types = [];
        if (showSchools) types.push('school');
        if (showASE) {
          types.push('ase_active', 'ase_historical', 'ase_planned');
        }
        if (showRedLight) {
          types.push('red_light_active', 'red_light_inactive');
        }

        if (types.length === 0) {
          setData(null);
          return;
        }

        const queryString = new URLSearchParams({
          types: types.join(','),
        }).toString();

        const url = `${ENFORCEMENT_SCHOOLS_URL}?${queryString}`;
        const response = await fetch(url);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const geojson = await response.json();
        setData(geojson);
      } catch (error) {
        console.error('Error fetching enforcement schools data:', error);
        onError(error);
      }
    };

    fetchData();
  }, [visible, showSchools, showASE, showRedLight, map, onError]);

  if (!visible || !data?.features) {
    return null;
  }

  return (
    <Source
      id="enforcement-schools-source"
      type="geojson"
      data={data}
    >
      {/* MARK: School Points - Bright Orange */}
      <Layer
        id={MAP_CONFIG.LAYER_IDS.SCHOOLS}
        type="circle"
        filter={['==', ['get', 'type'], 'school']}
        paint={{
          'circle-radius': 6,
          'circle-color': STYLE_CONSTANTS.COLORS.SCHOOL,
          'circle-stroke-width': 1.5,
          'circle-stroke-color': '#FFFFFF',
          'circle-opacity': 0.9,
        }}
      />

      {/* MARK: ASE Active - Red */}
      <Layer
        id={MAP_CONFIG.LAYER_IDS.ASE_ACTIVE}
        type="circle"
        filter={['==', ['get', 'type'], 'ase_camera', ['==', ['get', 'status'], 'Active']]}
        paint={{
          'circle-radius': 5,
          'circle-color': STYLE_CONSTANTS.COLORS.ASE_ACTIVE,
          'circle-stroke-width': 1,
          'circle-stroke-color': '#FFFFFF',
          'circle-opacity': 0.85,
        }}
      />

      {/* MARK: ASE Historical - Gray */}
      <Layer
        id={MAP_CONFIG.LAYER_IDS.ASE_HISTORICAL}
        type="circle"
        filter={['==', ['get', 'type'], 'ase_camera', ['==', ['get', 'status'], 'Historical']]}
        paint={{
          'circle-radius': 4,
          'circle-color': STYLE_CONSTANTS.COLORS.ASE_HISTORICAL,
          'circle-stroke-width': 0.5,
          'circle-stroke-color': '#999999',
          'circle-opacity': 0.5,
        }}
      />

      {/* MARK: ASE Planned - Yellow */}
      <Layer
        id={MAP_CONFIG.LAYER_IDS.ASE_PLANNED}
        type="circle"
        filter={['==', ['get', 'type'], 'ase_camera', ['==', ['get', 'status'], 'Planned']]}
        paint={{
          'circle-radius': 5,
          'circle-color': STYLE_CONSTANTS.COLORS.ASE_PLANNED,
          'circle-stroke-width': 1,
          'circle-stroke-color': '#FFFFFF',
          'circle-opacity': 0.75,
        }}
      />

      {/* MARK: Red Light Active - Green */}
      <Layer
        id={MAP_CONFIG.LAYER_IDS.RED_LIGHT_ACTIVE}
        type="circle"
        filter={['==', ['get', 'type'], 'red_light_camera', ['==', ['get', 'status'], 'Active']]}
        paint={{
          'circle-radius': 5,
          'circle-color': STYLE_CONSTANTS.COLORS.RED_LIGHT_ACTIVE,
          'circle-stroke-width': 1,
          'circle-stroke-color': '#FFFFFF',
          'circle-opacity': 0.85,
        }}
      />

      {/* MARK: Red Light Inactive - Light Gray */}
      <Layer
        id={MAP_CONFIG.LAYER_IDS.RED_LIGHT_INACTIVE}
        type="circle"
        filter={['==', ['get', 'type'], 'red_light_camera', ['==', ['get', 'status'], 'Decommissioned']]}
        paint={{
          'circle-radius': 4,
          'circle-color': STYLE_CONSTANTS.COLORS.RED_LIGHT_INACTIVE,
          'circle-stroke-width': 0.5,
          'circle-stroke-color': '#888888',
          'circle-opacity': 0.4,
        }}
      />
    </Source>
  );
}
