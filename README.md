# Treasury-Fiscal-Data

Production-style scraper and ingestion pipeline for USD interest rates, yield curves, and credit spreads.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Data Models](#data-models)
- [Storage](#storage)
- [Logging](#logging)
- [Data Readers](#data-readers)
- [Error Handling & Safety](#error-handling--safety)
- [Project Structure](#project-structure)
- [Development](#development)

---

## Overview

`main.py` (internally `rates_curves_spreads_scraper.py`) is a robust, production-oriented Python pipeline that fetches financial market data from multiple authoritative sources, validates it, and persists it to both local files and MongoDB.

### Targets

| Source | Data | API |
|--------|------|-----|
| U.S. Treasury FiscalData | Daily Treasury par yield curves | [FiscalData API v2](https://fiscaldata.treasury.gov/api-documentation/) |
| Federal Reserve Bank of St. Louis (FRED) | Treasury rates, spreads, reference rates | [FRED API](https://fred.stlouisfed.org/docs/api/fred/) |
| Federal Reserve Bank of New York | SOFR, EFFR, OBFR, TGCR, BGCR | [NY Fed Markets API](https://markets.newyorkfed.org/api/) |

### Design Goals

- **Safe HTTP reads**: Exponential-backoff retries, timeouts, bounded pagination, rate limiting, custom user-agent.
- **Safe parsing**: Pydantic schema validation, date parsing, numeric coercion, missing-value handling.
- **Safe writes**: Atomic local file writes, MongoDB upserts, run-level audit records.
- **Safe logging**: Daily folder rotation, 5,000-line log file caps.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   CLI / main()  │────▶│ IngestionPipeline│────▶│  Source Clients │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                              │                        │
                              ▼                        ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │   MongoWriter   │     │   FileWriter    │
                        │   (MongoDB)     │     │   (local CSV/   │
                        │                 │     │    JSON)        │
                        └─────────────────┘     └─────────────────┘
                              │
                              ▼
                        ┌─────────────────┐
                        │  DataReader     │
                        │ (read-back API) │
                        └─────────────────┘
```

### Core Classes

| Class | Responsibility |
|-------|-------------|
| `RotatingLineLogger` | JSON-structured logs with daily folders and 5,000-line rotation |
| `HttpClient` | Session-based HTTP with throttling, retries, and error handling |
| `FileWriter` | Atomic file writes (bytes, JSON, CSV) with SHA-256 checksums |
| `MongoWriter` | MongoDB connection, index management, bulk upserts |
| `SafeParser` | Defensive parsing of dates, floats, and DataFrame column checks |
| `FredClient` | FRED API adapter |
| `TreasuryFiscalDataClient` | Treasury FiscalData API adapter with pagination |
| `NewYorkFedClient` | NY Fed Markets API adapter with resilient JSON extraction |
| `SpreadBuilder` | Derives curve spreads from FRED rate observations |
| `IngestionPipeline` | Orchestrates the entire fetch→validate→store workflow |
| `DataReader` | Safe downstream readers for MongoDB with shape validation |

---

## Installation

### Requirements

- Python 3.10+
- MongoDB (optional; pipeline works locally without it, but expects connection)
- FRED API key (optional; required for FRED data)

### Dependencies

```bash
pip install requests pandas pymongo python-dotenv pydantic tenacity beautifulsoup4 lxml
```

Or create a `requirements.txt`:

```text
requests>=2.31.0
pandas>=2.0.0
pymongo>=4.6.0
python-dotenv>=1.0.0
pydantic>=2.0.0
tenacity>=8.2.0
beautifulsoup4>=4.12.0
lxml>=4.9.0
```

---

## Configuration

Create a `.env` file in the project root:

```dotenv
# MongoDB (optional — defaults to localhost)
MONGO_URI=mongodb://localhost:27017
MONGO_DB=rates_curves_spreads

# FRED API key (get one at https://fred.stlouisfed.org/docs/api/api_key.html)
FRED_API_KEY=your_fred_api_key_here
```

Environment variables are read automatically; the pipeline will skip FRED if `FRED_API_KEY` is absent.

---

## Usage

### Basic Run

```bash
python main.py --start 1990-01-01 --end 2026-05-24
```

### Skip Specific Sources

```bash
# Skip FRED (useful if you don't have an API key)
python main.py --start 2024-01-01 --end 2026-05-24 --skip-fred

# Skip Treasury curves
python main.py --start 2024-01-01 --end 2026-05-24 --skip-treasury

# Skip NY Fed reference rates
python main.py --start 2024-01-01 --end 2026-05-24 --skip-nyfed

# Skip everything except Treasury
python main.py --start 2024-01-01 --end 2026-05-24 --skip-fred --skip-nyfed
```

### Date Constraints

- `--start` and `--end` are required.
- Format: `YYYY-MM-DD`
- `--start` must not be later than `--end`.

---

## Data Sources

### 1. FRED — Treasury Rates & Spreads

Fetched series (all daily, in percent):

| Series ID | Category | Description |
|-----------|----------|-------------|
| `DGS1MO`–`DGS30` | `treasury_rate` | Constant maturity Treasury yields (1M–30Y) |
| `T10Y2Y` | `curve_spread` | 10Y Treasury minus 2Y Treasury |
| `T10Y3M` | `curve_spread` | 10Y Treasury minus 3M Treasury |
| `BAA10Y` | `credit_spread` | Moody's Baa minus 10Y Treasury |
| `BAMLC0A0CM` | `credit_spread` | ICE BofA US Corporate Index OAS |
| `BAMLH0A0HYM2` | `credit_spread` | ICE BofA US High Yield Index OAS |
| `SOFR` | `money_market` | Secured Overnight Financing Rate |
| `EFFR` | `money_market` | Effective Federal Funds Rate |

**Derived spreads** (computed locally from FRED rates):

| Spread ID | Long Leg | Short Leg |
|-----------|----------|-----------|
| `UST_10Y_2Y_DERIVED` | `DGS10` | `DGS2` |
| `UST_10Y_3M_DERIVED` | `DGS10` | `DGS3MO` |
| `UST_5Y_2Y_DERIVED` | `DGS5` | `DGS2` |
| `UST_30Y_10Y_DERIVED` | `DGS30` | `DGS10` |

### 2. U.S. Treasury — Daily Par Yield Curve

Endpoint: `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/daily_treasury_rates`

Tenors mapped:

| API Field | Label | Years |
|-----------|-------|-------|
| `bc_1month` | 1M | 0.083 |
| `bc_2month` | 2M | 0.167 |
| `bc_3month` | 3M | 0.250 |
| `bc_4month` | 4M | 0.333 |
| `bc_6month` | 6M | 0.500 |
| `bc_1year` | 1Y | 1.0 |
| `bc_2year` | 2Y | 2.0 |
| `bc_3year` | 3Y | 3.0 |
| `bc_5year` | 5Y | 5.0 |
| `bc_7year` | 7Y | 7.0 |
| `bc_10year` | 10Y | 10.0 |
| `bc_20year` | 20Y | 20.0 |
| `bc_30year` | 30Y | 30.0 |

### 3. NY Fed — Reference Rates

| Rate | Endpoint Path |
|------|--------------|
| SOFR | `/rates/secured/sofr/search.json` |
| EFFR | `/rates/unsecured/effr/search.json` |
| OBFR | `/rates/unsecured/obfr/search.json` |
| TGCR | `/rates/secured/tgcr/search.json` |
| BGCR | `/rates/secured/bgcr/search.json` |

---

## Data Models

All data is validated with **Pydantic v2** before storage.

### `RateObservation`

| Field | Type | Notes |
|-------|------|-------|
| `series_id` | `str` | e.g. `DGS10`, `SOFR` |
| `source` | `str` | `FRED`, `NEW_YORK_FED_MARKETS_API` |
| `date` | `date` | ISO format in MongoDB |
| `value` | `float` | Must be between -100 and 100 |
| `unit` | `str` | Default: `percent` |
| `frequency` | `str` | Default: `daily` |
| `category` | `str` | `treasury_rate`, `curve_spread`, etc. |
| `description` | `Optional[str]` | Human-readable label |
| `created_at` | `datetime` | UTC timestamp |

### `CurveSnapshot`

| Field | Type | Notes |
|-------|------|-------|
| `curve_id` | `str` | e.g. `UST_PAR_NOMINAL` |
| `source` | `str` | `US_TREASURY_FISCALDATA` |
| `date` | `date` | Snapshot date |
| `currency` | `str` | Default: `USD` |
| `curve_type` | `str` | Default: `par_yield` |
| `unit` | `str` | Default: `percent` |
| `points` | `List[CurvePoint]` | Minimum 3 points, sorted by maturity |
| `created_at` | `datetime` | UTC timestamp |

### `CurvePoint`

| Field | Type | Notes |
|-------|------|-------|
| `tenor` | `str` | e.g. `10Y` |
| `years` | `float` | Maturity in years |
| `rate` | `float` | Yield value |

### `SpreadObservation`

| Field | Type | Notes |
|-------|------|-------|
| `spread_id` | `str` | e.g. `UST_10Y_2Y_DERIVED` |
| `source` | `str` | `DERIVED_FROM_RATE_OBSERVATIONS` |
| `date` | `date` | Observation date |
| `value` | `float` | Spread value |
| `unit` | `str` | Default: `percent` |
| `legs` | `Dict[str, str]` | `{"long_leg": "...", "short_leg": "..."}` |
| `created_at` | `datetime` | UTC timestamp |

---

## Storage

### Local Files

| Directory | Content |
|-----------|---------|
| `data/raw/fred/` | Raw JSON responses from FRED |
| `data/raw/treasury/` | Raw JSON responses from Treasury API (paginated) |
| `data/raw/nyfed/` | Raw JSON responses from NY Fed |
| `data/processed/` | CSV exports of validated data |
| `data/curves/` | Reserved for curve-specific outputs |
| `logs/YYYY-MM-DD/` | Daily log folders with `log-0001.log`, `log-0002.log`, ... |

All local writes are **atomic** (written to `.tmp` then renamed) to prevent partial files on crashes.

### MongoDB Collections

| Collection | Index | Purpose |
|------------|-------|---------|
| `rate_observations` | `{series_id: 1, date: 1}` (unique) | FRED & NY Fed rates |
| `curve_snapshots` | `{curve_id: 1, date: 1}` (unique) | Treasury yield curves |
| `spread_observations` | `{spread_id: 1, date: 1}` (unique) | Derived spreads |
| `ingestion_runs` | `{run_id: 1}` (unique) | Pipeline run audit records |

Dates are stored as ISO strings in MongoDB for predictable querying across drivers.

---

## Logging

The pipeline uses a custom `RotatingLineLogger` that writes **JSON Lines** (one JSON object per line):

```json
{"ts": "2026-05-24T20:53:24.836816+00:00", "level": "INFO", "event": "ingestion_started", "run_id": "...", "start": "1990-01-01", "end": "2026-05-24"}
```

- **Daily folders**: `logs/2026-05-24/`
- **5,000-line rotation**: `log-0001.log`, `log-0002.log`, ...
- **Auto-continuation**: If restarted, picks up the next file index.

Every pipeline run produces an audit record in MongoDB (`ingestion_runs`) capturing:
- `run_id`, `started_at`, `ended_at`
- `status`: `success`, `partial_failure`, or `failure`
- `records_inserted_or_matched`, `failure_count`
- `sources` list and date range

---

## Data Readers

After ingestion, use `DataReader` to query the database safely:

```python
from main import MongoWriter, DataReader

mongo = MongoWriter("mongodb://localhost:27017", "rates_curves_spreads")
reader = DataReader(mongo)

# Read a single rate series
dgs10 = reader.read_rate_series("DGS10", start="2024-01-01", end="2026-05-24")
print(dgs10.head())

# Read flattened curve snapshots
curves = reader.read_curve_snapshots("UST_PAR_NOMINAL", start="2024-01-01")
print(curves.head())

# Get a wide matrix: dates × tenors
matrix = reader.curve_matrix("UST_PAR_NOMINAL", start="2024-01-01")
print(matrix.head())
```

All readers enforce:
- Column existence checks
- Date parsing and numeric coercion
- Deduplication (`keep="last"`)
- Sorted output

---

## Error Handling & Safety

| Layer | Safeguard |
|-------|-----------|
| **HTTP** | Exponential backoff with jitter (1–30s), 5 retries, rate limiting (0.25s between requests) |
| **Pagination** | Hard cap of 500 pages for Treasury API |
| **Parsing** | Defensive date/float parsing; missing/invalid values skipped with warnings |
| **Validation** | Pydantic models reject out-of-range values (`< -100` or `> 100`) and NaNs |
| **Storage** | Atomic file writes; MongoDB bulk upserts with `ordered=False` |
| **Resilience** | Per-series/per-rate error isolation; one failure does not abort the entire pipeline |
| **Audit** | Every run recorded in `ingestion_runs` with status and counts |

---

## Project Structure

```
Treasury-Fiscal-Data/
├── main.py              # Pipeline script (937 lines)
├── README.md            # This file
├── .env                 # Environment variables (not committed)
├── .gitignore           # Git ignore rules
├── data/
│   ├── curves/          # Curve outputs
│   ├── processed/       # Validated CSV exports
│   └── raw/             # Raw API responses
│       ├── fred/
│       ├── nyfed/
│       └── treasury/
└── logs/
    └── YYYY-MM-DD/      # Daily JSON log files
```

---

## Development

### Running Locally Without MongoDB

If MongoDB is unavailable, the script will fail on connection. For local-only CSV/JSON output, modify `main()` to skip `MongoWriter` initialization (not recommended for production).

### Extending the Pipeline

#### Add a New FRED Series

Edit `FRED_SERIES` in `main.py`:

```python
SeriesSpec("FRED", "NEW_ID", "category", "Description"),
```

#### Add a New Derived Spread

Edit `SpreadBuilder.definitions`:

```python
"MY_SPREAD": ("LONG_SERIES", "SHORT_SERIES"),
```

#### Add a New NY Fed Rate

Edit `NewYorkFedClient.ENDPOINTS`:

```python
"NEW_RATE": "/rates/.../search.json",
```

### Testing

A test suite is recommended. Verify the script end-to-end with a short date range:

```bash
python main.py --start 2026-05-20 --end 2026-05-24 --skip-fred
```

Check `logs/` for structured output and `data/raw/` for preserved API responses.

---

## License

See [LICENSE](./LICENSE).
