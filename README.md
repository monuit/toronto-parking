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

## 🤖 OpenAI Forecasting Roadmap

### Historical Coverage & Holdouts

- **Source window:** Parking and officer activity data from **2008 through 2024**.
- **Per-year evaluation slices:** Reserve the **last quarter of each year (Oct–Dec)** plus **one floating high-demand month** (e.g., March) for testing to capture seasonal variance.
- **Training inputs:** Remaining months per year (≈ 8 months) feed the fine-tuning corpora.
- **Targets:** Predict (a) ticket counts per geocoded location per hour and (b) officer patrol density vectors derived from historical movement logs.

### Dataset Construction

- Extend existing preprocessing flows to emit **OpenAI-ready JSONL** files with chat-style prompts containing spatial/temporal context and tool-call friendly completions.
- Generate paired datasets for **training** and **evaluation**; include metadata fields (`year`, `month`, `location_id`, `officer_cluster`) to support stratified sampling.
- Store artifacts under `output/fine_tuning/{year}/` with manifest JSON describing splits and checksum hashes.

### Fine-Tune & Eval Workflow

- Automate fine-tuning via CLI scripts that:
   1. Upload curated JSONL files to OpenAI file storage.
   2. Launch fine-tune jobs targeting `gpt-4.1-mini` (tickets) and `gpt-4o-mini` (officer movement) with consistent hyperparameters.
   3. Monitor job status, persisting run IDs and metrics to `output/fine_tuning/runs.json`.
- Configure **OpenAI Evals** to benchmark against holdout months, scoring MAE, RMSE, and hotspot ranking accuracy.
- Promote models only when eval metrics beat the naïve historical-baseline funnel by ≥10%.

### Forecast Generation (Oct 7, 2025 Focus)

- Implement a forecast harness that:
   1. Builds scenario prompts from the latest geocoded ticket map and officer routing history.
   2. Calls the fine-tuned models to obtain **hourly forecasts for Oct 7, 2025** (primary test case) and optional adjacent days.
   3. Aggregates outputs into GeoJSON + timeseries JSON persisted under `map-app/public/data/forecasts/`.
- Cache model responses with idempotent keys `(model_id, scenario_hash)` to avoid duplicate billing.

### Frontend & Monitoring

- Extend the MapLibre frontend to ingest the saved forecast GeoJSON, providing toggles for "Historicals" vs. "Oct 7 2025 Forecast".
- Display confidence intervals from eval metrics and flag locations where model uncertainty >25%.
- Log forecast generation runs (input hashes, model versions, timestamps) to support reproducibility and rollback.

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

# Build fine-tuning datasets (train/test_case/seasonal splits)
python preprocessing/build_fine_tune_datasets.py --output-root output/fine_tuning

# Launch fine-tune + evaluation workflow (requires OPENAI_API_KEY)
python preprocessing/manage_fine_tunes.py run-full-cycle --train output/fine_tuning/aggregated/train.jsonl --validation output/fine_tuning/aggregated/test_case.jsonl --eval-dataset output/fine_tuning/aggregated/seasonal.jsonl --model gpt-4.1-mini
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

```bash
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

```text
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
