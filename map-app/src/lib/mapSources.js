/**
 * Map configuration and data sources
 * Single responsibility: centralize all map-related URLs and constants
 */
import {
  TILE_URL_TEMPLATE,
  TILE_LAYER_NAME,
  SUMMARY_API_PATH,
  CLUSTER_EXPANSION_API_PATH,
  HEATMAP_API_PATH,
  RAW_POINT_ZOOM_THRESHOLD,
  SUMMARY_ZOOM_THRESHOLD,
} from '../../shared/mapConstants.js';

export const MAP_CONFIG = {
  // Toronto city center
  DEFAULT_VIEW: {
    longitude: -79.3832,
    latitude: 43.6532,
    zoom: 11
  },

  // Base map style - OpenMapTiles Basic
  STYLE_URL: '/styles/basic-style.json',

  // Zoom thresholds for detail levels
  ZOOM_THRESHOLDS: {
    SHOW_CLUSTERS: 8,
    SHOW_INDIVIDUAL_TICKETS: RAW_POINT_ZOOM_THRESHOLD,
    SUMMARY_MIN: SUMMARY_ZOOM_THRESHOLD,
    SHOW_DETAILED_INFO: 16
  },

  // Layer IDs for programmatic control
  LAYER_IDS: {
    NEIGHBOURHOODS_FILL: 'neighbourhoods-fill',
    NEIGHBOURHOODS_OUTLINE: 'neighbourhoods-outline',
    NEIGHBOURHOODS_LABEL: 'neighbourhoods-label',
    CITY_GLOW_SOFT: 'tickets-glow-soft',
    CITY_GLOW_CORE: 'tickets-glow-core',
    TICKETS_CLUSTER: 'tickets-cluster',
    TICKETS_CLUSTER_COUNT: 'tickets-cluster-count',
    TICKETS_POINTS: 'tickets-points'
  },

  // Source IDs
  SOURCE_IDS: {
    NEIGHBOURHOODS: 'toronto-neighbourhoods',
    TICKETS: 'toronto-tickets',
    CITY_GLOW: 'tickets-glow'
  },

  SOURCE_LAYERS: {
    TICKETS: TILE_LAYER_NAME
  },

  // Data paths
  DATA_PATHS: {
    NEIGHBOURHOODS: '/data/neighbourhoods.geojson',
    OFFICER_STATS: '/data/officer_stats.json',
    NEIGHBOURHOOD_STATS: '/data/neighbourhood_stats.json',
    CITY_GLOW_LINES: '/data/tickets_glow_lines.geojson',
    CENTRELINE_LOOKUP: '/data/centreline_lookup.json'
  },

  TILE_SOURCE: {
    TICKETS: TILE_URL_TEMPLATE
  },

  API_PATHS: {
    SUMMARY: SUMMARY_API_PATH,
    CLUSTER_EXPANSION: CLUSTER_EXPANSION_API_PATH,
    HEATMAP: HEATMAP_API_PATH
  }
};

export const STYLE_CONSTANTS = {
  // Apple-ish color palette
  COLORS: {
    NEIGHBOURHOOD_FILL: 'rgba(100, 149, 237, 0.2)',
    NEIGHBOURHOOD_OUTLINE: 'rgba(70, 130, 180, 0.8)',
    NEIGHBOURHOOD_HOVER: 'rgba(100, 149, 237, 0.4)',
    TICKET_POINT: '#FF6B6B',
    TICKET_CLUSTER: '#4ECDC4',
    BACKGROUND: '#F8F9FA'
  },

  // Choropleth color scale (tickets per capita)
  CHOROPLETH_STOPS: [
    [0, '#E8F5E9'],
    [10, '#A5D6A7'],
    [20, '#66BB6A'],
    [50, '#43A047'],
    [100, '#2E7D32'],
    [200, '#1B5E20']
  ],

  // City glow line color stops (tickets per 100m)
  CITY_GLOW_STOPS: [
    { value: 250,   color: '#C8DAFF', label: '250' },
    { value: 500,   color: '#AFC8FF', label: '500' },
    { value: 1000,  color: '#96B3FF', label: '1k' },
    { value: 1500,  color: '#849EFA', label: '1.5k' },
    { value: 3000,  color: '#7A89F0', label: '3k' },
    { value: 6500,  color: '#9B6DD7', label: '6.5k' },
    { value: 10000, color: '#CF58AD', label: '10k' },
    { value: 12500, color: '#E0529C', label: '12.5k' },
    { value: 15000, color: '#F26A7C', label: '15k' },
    { value: 20000, color: '#FF5C5C', label: '20k' }
  ]
};
