// Legacy compatibility helper. The application now renders the glow layer via
// `CityGlowLayer`, but we keep this re-export to avoid build failures for any
// deferred imports that still reference `HeatmapLayer`.
export { CityGlowLayer as HeatmapLayer } from './CityGlowLayer.jsx';
