# Toronto Parking Tickets Analysis & Mapping

**Interactive map visualization of 26.5 million Toronto parking tickets (2011-2024)**

## 🚀 Quick Start

```powershell
# 1. Check geocoding progress
python geocoding/geocoding_status.py

# 2. Continue fast geocoding (3.7 queries/second)
python geocoding/run_geocoding_fast.py

# 3. Retry failed addresses
python geocoding/retry_failed_geocoding.py

# 4. View map
cd map-app
npm run dev
```

## 📊 Project Stats

- **Total Tickets:** 26,543,869 (2011-2024)
- **Unique Addresses:** 676,782
- **Geocoding Speed:** 3.7 queries/second
- **Current Progress:** 1,000 addresses geocoded (408 successful)
- **Pattern Discovered:** 99.6% same tag number, 0.4% +1

## 📁 Project Structure

```
toronto-parking/
├── docs/                      # Documentation
├── preprocessing/             # Data preparation
├── geocoding/                 # Geocoding engines
├── analysis/                  # Data analysis
├── output/                    # Generated files
├── map-app/                   # React frontend
└── tickets_data/              # Raw CSV files
```

## 🎯 Current Status

### ✅ Completed

- Data download (26.5M tickets)
- Pattern analysis (99.6% same number)
- Map application (React + MapLibre)
- Fast batch geocoding system (3.7 queries/sec)
- Retry mechanism for failures
- Comprehensive documentation

### 🟡 In Progress

- Full geocoding run (676,782 addresses)

### 🔜 Next Steps

1. Extract all unique addresses
2. Run full geocoding (~2 days)
3. Integrate into map visualization

## 💻 Commands Reference

### Geocoding (Main Workflow)

```powershell
# Fast batch geocoding (RECOMMENDED)
python geocoding/run_geocoding_fast.py

# Check progress
python geocoding/geocoding_status.py

# Retry failed addresses
python geocoding/retry_failed_geocoding.py
```

### Preprocessing

```powershell
# Extract all unique addresses from CSVs
python preprocessing/prepare_map_data.py

# Create test dataset
python preprocessing/create_test_queries.py
```

### Map Application

```powershell
cd map-app
npm install      # First time only
npm run dev      # Start development server
```

## 🚀 Performance

### Fast Batch Geocoder

- **Speed:** 3.7 queries/second
- **Batch Size:** 50 addresses
- **Concurrency:** 10 simultaneous requests
- **Checkpoints:** Every 500 addresses
- **Resume:** Automatic (skips completed)

### Time Estimates

| Addresses | Fast Geocoder      | Conservative |
| --------- | ------------------ | ------------ |
| 1,000     | ~4.5 minutes       | ~11 minutes  |
| 100,000   | ~7.5 hours         | ~19 hours    |
| 676,782   | ~51 hours (2 days) | ~5 days      |

## 🔧 Configuration

### API Setup

Create `.env` file:

```
GEOCODE_MAPS_CO_API_KEY=your_key_here
```

### Dependencies

```powershell
# Python packages
pip install pandas numpy matplotlib requests aiohttp python-dotenv

# Node.js packages (for map-app)
cd map-app
npm install
```

## 📚 Documentation

- **Complete Guide:** `docs/GEOCODING_COMPLETE_DOCUMENTATION.md`
- **File Organization:** `docs/FILE_ORGANIZATION.md`
- **API Docs:** `docs/GEOCODING_README.md`
- **Map Setup:** `docs/MAP_APP_SETUP.md`

## 🎯 Workflow

```
1. prepare_map_data.py
   ↓ generates unique_queries.json

2. run_geocoding_fast.py
   ↓ creates geocoding_results.json

3. geocoding_status.py
   ↓ check progress

4. retry_failed_geocoding.py
   ↓ retry failures (optional)

5. prepare_map_data.py
   ↓ integrates coords into tickets_aggregated.geojson

6. map-app (npm run dev)
   ✅ View interactive map!
```

## 🐛 Troubleshooting

### High failure rate

- Normal for fast geocoder (rate limits)
- Most failures are retryable
- Run `python geocoding/retry_failed_geocoding.py`

### Script crashes

- Progress auto-saved to `output/geocoding_results.json`
- Just re-run same script to resume

### Map shows points in water

- Geocoding not complete
- Run `python preprocessing/prepare_map_data.py` to integrate results

---

**Last Updated:** October 6, 2025  
**Status:** Fast geocoding system operational, ready for full run  
**Next:** Run full geocoding for all 676,782 addresses

For detailed documentation, see `docs/GEOCODING_COMPLETE_DOCUMENTATION.md`
