# ⚓ AIS Voyage Engine

A professional-grade, cloud-native data pipeline that transforms raw NOAA AIS pings into enriched maritime voyage insights.

## 🏗 Architecture (Medallion)
- **Bronze Layer**: Raw AIS pings filtered by port proximity (5km) using Polars & SciPy cKDTree.
- **Silver Layer**: Stitched port events (Arrivals/Departures) using dbt & SQL.
- **Gold Layer**: Enriched voyages with maritime distances calculated via dbt Python models.

## 🛠 Tech Stack
- **Orchestration**: GitHub Actions
- **Data Warehouse**: MotherDuck (Cloud DuckDB)
- **Transformation**: dbt (Data Build Tool)
- **Visualization**: Evidence.dev

## 🚀 Getting Started
1. **Environment Setup**:

   ```bash
   export MOTHERDUCK_TOKEN="your_token_here"
   uv sync --group dev        # installs runtime + dev tooling (ruff, mypy, pytest, pre-commit)
   ```

2. **Run Pipeline (Manual Backfill)**:

   ```bash
   # Ingest entire year of 2025
   uv run python src/ingest_motherduck.py --year 2025

   # Ingest specific month
   uv run python src/ingest_motherduck.py --year 2025 --month 1

   # Transform data (Silver -> Gold)
   uv run dbt build --target prod
   ```

3. **Run Pipeline (Daily Automation)**:

   ```bash
   # Defaults to yesterday's data
   uv run python src/ingest_motherduck.py
   uv run dbt build --target prod
   ```

4. **Live supplementary source (aisstream.io)**:

   ```bash
   export AISSTREAM_API_KEY="your_key_here"   # register free at https://aisstream.io
   uv run python -m src.ingest_motherduck --source aisstream --collect-seconds 60 --dry-run
   ```

   aisstream.io is a **beta service with no SLA and no published redistribution
   ToS** — it is a supplementary, real-time enrichment layer only, never the
   basis of the public-facing product/API until legal clears the licensing
   ambiguity (Leila-gated). See `src/ais_sources.py::AisstreamSource` docstring.

## 🧪 Testing
Run the test suite locally:

```bash
uv run pytest tests/
