# Toronto Parking Tickets Map Visualization

An interactive map-based visualization of Toronto's parking ticket data from 2008-2024, featuring 26.5+ million parking tickets with officer and neighbourhood leaderboards.

## 🎯 Features

- **Interactive Map**: Smooth pan/zoom with MapLibre GL JS
- **Neighbourhood Overlays**: View parking ticket density by neighbourhood
- **Clustered Points**: Individual tickets with intelligent clustering
- **Officer Leaderboard**: Top 100 officers by ticket count
- **Neighbourhood Leaderboard**: Top 10 neighbourhoods by violations
- **Real-time Filters**: Toggle layers on/off
- **Responsive Design**: Apple-inspired UI that works on desktop and mobile

## 🏗️ Architecture

Built following strict code quality guidelines:

- **Files**: All under 500 lines
- **Single Responsibility Principle**: Each component has one clear purpose
- **Modular Design**: Components are reusable like Lego blocks

### Component Structure

```
map-app/
├── src/
│   ├── components/         # UI components (each <200 lines)
│   │   ├── MapContainer.jsx
│   │   ├── NeighbourhoodLayer.jsx
│   │   ├── PointsLayer.jsx
│   │   ├── OfficerLeaderboard.jsx
│   │   ├── NeighbourhoodLeaderboard.jsx
│   │   ├── LayerToggles.jsx
│   │   ├── Legend.jsx
│   │   └── InfoPopup.jsx
│   ├── lib/                # Utilities
│   │   ├── mapSources.js
│   │   └── dataTransforms.js
│   ├── hooks/              # Custom React hooks
│   │   ├── useMapReady.js
│   │   └── useWindowSize.js
│   └── styles/             # CSS modules
└── public/data/            # GeoJSON & JSON data
```

## 🚀 Quick Start

### Prerequisites

- Node.js 18+
- Python 3.13+ (for data processing)

### Installation

1. **Install dependencies**:

   ```bash
   cd map-app
   npm install
   ```

2. **Prepare data** (run from parent directory):

   ```bash
   python prepare_map_data.py
   ```

3. **Start development server**:

   ```bash
   npm run dev
   ```

4. **Open browser**: Navigate to `http://localhost:5173`

## 📊 Data Pipeline

The data preparation script (`prepare_map_data.py`) processes all parking ticket CSVs and generates GeoJSON files for map rendering.

## 🎨 Design Philosophy

**Apple-inspired visual design**: Soft grays, subtle pastels, high contrast overlays, smooth transitions, clean minimalist UI.

## 📄 License

Data: City of Toronto Open Data Portal  
Map tiles: © OpenStreetMap contributors
