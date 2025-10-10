export const SUMMARY_ZOOM_THRESHOLD = 12;
export const RAW_POINT_ZOOM_THRESHOLD = 14;
export const TICKET_TILE_MIN_ZOOM = RAW_POINT_ZOOM_THRESHOLD;

export const TILE_LAYER_NAME = 'parking_tickets';
export const TILE_URL_TEMPLATE = '/tiles/{z}/{x}/{y}.pbf?dataset={dataset}';

export const SUMMARY_API_PATH = '/api/map-summary';
export const CLUSTER_EXPANSION_API_PATH = '/api/cluster-expansion';
export const HEATMAP_API_PATH = '/api/heatmap-points';
export const DATASET_TOTALS_API_PATH = '/api/dataset-totals';
export const YEARLY_YEARS_API_PATH = '/api/yearly/years';
export const YEARLY_TOTALS_API_PATH = '/api/yearly/totals';
export const YEARLY_TOP_STREETS_API_PATH = '/api/yearly/top-streets';
export const YEARLY_TOP_NEIGHBOURHOODS_API_PATH = '/api/yearly/top-neighbourhoods';
export const YEARLY_TOP_LOCATIONS_API_PATH = '/api/yearly/top-locations';
export const YEARLY_LOCATION_DETAIL_API_PATH = '/api/yearly/location';
export const YEARLY_TOP_GROUPS_API_PATH = '/api/yearly/top-groups';
export const WARD_SUMMARY_API_PATH = '/api/wards/summary';
export const WARD_GEOJSON_API_PATH = '/api/wards/geojson';
export const WARD_PREWARM_API_PATH = '/api/wards/prewarm';
export const WARD_TILE_URL_TEMPLATE = '/tiles/wards/{dataset}/{z}/{x}/{y}.pbf';
export const WARD_TILE_SOURCE_LAYER = 'ward_polygons';

export const WARD_CHOROPLETH_STOPS = [
  [0, '#f1eef6'],
  [1000, '#d0d1e6'],
  [5000, '#a6bddb'],
  [15000, '#74a9cf'],
  [30000, '#3690c0'],
  [60000, '#0570b0'],
  [100000, '#034e7b'],
];