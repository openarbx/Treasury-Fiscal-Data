"""
rates_curves_spreads_scraper.py

Production-style scraper/ingestion pipeline for USD rates, curves, and spreads.

Targets:
- U.S. Treasury FiscalData API for Treasury curve snapshots.
- New York Fed Markets API for SOFR/EFFR/OBFR/TGCR/BGCR-style reference rates.
- FRED API for spread/rate time series, when you provide a FRED API key.

Design goals:
- Safe HTTP reads: retries, timeouts, bounded pagination, rate limiting, user agent.
- Safe parsing: schema validation, date parsing, numeric coercion, missing-value handling.
- Safe writes: atomic local writes, MongoDB upserts, run-level audit records.
- Safe logging: daily folder, one log file per 5,000 lines.

Install:
    pip install requests pandas pymongo python-dotenv pydantic tenacity beautifulsoup4 lxml

Environment variables:
    MONGO_URI=mongodb://localhost:27017
    MONGO_DB=rates_curves_spreads
    FRED_API_KEY=your_fred_api_key_optional

Run:
    python rates_curves_spreads_scraper.py --start 1990-01-01 --end 2026-05-24
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from pydantic import BaseModel, Field, ValidationError, field_validator
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter


# ============================================================
# Configuration
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
CURVE_DIR = DATA_DIR / "curves"
LOG_DIR = BASE_DIR / "logs"

for folder in [RAW_DIR, PROCESSED_DIR, CURVE_DIR, LOG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class SeriesSpec:
    source: str
    series_id: str
    category: str
    description: str
    unit: str = "percent"
    frequency: str = "daily"


FRED_SERIES: List[SeriesSpec] = [
    SeriesSpec("FRED", "DGS1MO", "treasury_rate", "1-Month Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS3MO", "treasury_rate", "3-Month Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS6MO", "treasury_rate", "6-Month Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS1", "treasury_rate", "1-Year Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS2", "treasury_rate", "2-Year Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS5", "treasury_rate", "5-Year Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS7", "treasury_rate", "7-Year Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS10", "treasury_rate", "10-Year Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS20", "treasury_rate", "20-Year Treasury Constant Maturity"),
    SeriesSpec("FRED", "DGS30", "treasury_rate", "30-Year Treasury Constant Maturity"),
    SeriesSpec("FRED", "T10Y2Y", "curve_spread", "10-Year Treasury Minus 2-Year Treasury"),
    SeriesSpec("FRED", "T10Y3M", "curve_spread", "10-Year Treasury Minus 3-Month Treasury"),
    SeriesSpec("FRED", "BAA10Y", "credit_spread", "Moody's Baa Corporate Bond Yield Minus 10-Year Treasury"),
    SeriesSpec("FRED", "BAMLC0A0CM", "credit_spread", "ICE BofA US Corporate Index OAS"),
    SeriesSpec("FRED", "BAMLH0A0HYM2", "credit_spread", "ICE BofA US High Yield Index OAS"),
    SeriesSpec("FRED", "SOFR", "money_market", "Secured Overnight Financing Rate"),
    SeriesSpec("FRED", "EFFR", "money_market", "Effective Federal Funds Rate"),
]

TREASURY_CURVE_FIELDS: Dict[str, float] = {
    "bc_1month": 1.0 / 12.0,
    "bc_2month": 2.0 / 12.0,
    "bc_3month": 3.0 / 12.0,
    "bc_4month": 4.0 / 12.0,
    "bc_6month": 6.0 / 12.0,
    "bc_1year": 1.0,
    "bc_2year": 2.0,
    "bc_3year": 3.0,
    "bc_5year": 5.0,
    "bc_7year": 7.0,
    "bc_10year": 10.0,
    "bc_20year": 20.0,
    "bc_30year": 30.0,
}

TREASURY_TENOR_LABELS: Dict[str, str] = {
    "bc_1month": "1M",
    "bc_2month": "2M",
    "bc_3month": "3M",
    "bc_4month": "4M",
    "bc_6month": "6M",
    "bc_1year": "1Y",
    "bc_2year": "2Y",
    "bc_3year": "3Y",
    "bc_5year": "5Y",
    "bc_7year": "7Y",
    "bc_10year": "10Y",
    "bc_20year": "20Y",
    "bc_30year": "30Y",
}


# ============================================================
# Logging with 5,000-line rotation
# ============================================================

class RotatingLineLogger:
    def __init__(self, root_dir: Path, max_lines: int = 5000) -> None:
        self.root_dir = root_dir
        self.max_lines = max_lines
        self.current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.date_dir = self.root_dir / self.current_date
        self.date_dir.mkdir(parents=True, exist_ok=True)
        self.file_index = self._next_file_index()
        self.line_count = 0
        self.file_path = self.date_dir / f"log-{self.file_index:04d}.log"

    def _next_file_index(self) -> int:
        existing = sorted(self.date_dir.glob("log-*.log"))
        if not existing:
            return 1
        last = existing[-1].stem.split("-")[-1]
        try:
            return int(last) + 1
        except ValueError:
            return 1

    def _roll_if_needed(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.current_date:
            self.current_date = today
            self.date_dir = self.root_dir / today
            self.date_dir.mkdir(parents=True, exist_ok=True)
            self.file_index = self._next_file_index()
            self.line_count = 0
            self.file_path = self.date_dir / f"log-{self.file_index:04d}.log"
            return

        if self.line_count >= self.max_lines:
            self.file_index += 1
            self.line_count = 0
            self.file_path = self.date_dir / f"log-{self.file_index:04d}.log"

    def log(self, level: str, event: str, **kwargs: Any) -> None:
        self._roll_if_needed()
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level.upper(),
            "event": event,
            **kwargs,
        }
        with self.file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
        self.line_count += 1

    def info(self, event: str, **kwargs: Any) -> None:
        self.log("INFO", event, **kwargs)

    def warning(self, event: str, **kwargs: Any) -> None:
        self.log("WARNING", event, **kwargs)

    def error(self, event: str, **kwargs: Any) -> None:
        self.log("ERROR", event, **kwargs)


LOGGER = RotatingLineLogger(LOG_DIR)


# ============================================================
# Validation models
# ============================================================

class RateObservation(BaseModel):
    series_id: str
    source: str
    date: date
    value: float
    unit: str = "percent"
    frequency: str = "daily"
    category: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("value")
    @classmethod
    def finite_value(cls, value: float) -> float:
        if pd.isna(value):
            raise ValueError("value cannot be NaN")
        if value < -100 or value > 100:
            raise ValueError(f"suspicious rate/spread value: {value}")
        return float(value)


class CurvePoint(BaseModel):
    tenor: str
    years: float
    rate: float

    @field_validator("rate")
    @classmethod
    def finite_rate(cls, value: float) -> float:
        if pd.isna(value):
            raise ValueError("rate cannot be NaN")
        if value < -100 or value > 100:
            raise ValueError(f"suspicious curve rate: {value}")
        return float(value)


class CurveSnapshot(BaseModel):
    curve_id: str
    source: str
    date: date
    currency: str = "USD"
    curve_type: str = "par_yield"
    unit: str = "percent"
    points: List[CurvePoint]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("points")
    @classmethod
    def enough_points(cls, points: List[CurvePoint]) -> List[CurvePoint]:
        if len(points) < 3:
            raise ValueError("curve snapshot has fewer than 3 valid points")
        years = [p.years for p in points]
        if years != sorted(years):
            raise ValueError("curve points must be sorted by maturity")
        return points


class SpreadObservation(BaseModel):
    spread_id: str
    source: str
    date: date
    value: float
    unit: str = "percent"
    legs: Dict[str, str]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("value")
    @classmethod
    def finite_spread(cls, value: float) -> float:
        if pd.isna(value):
            raise ValueError("spread cannot be NaN")
        if value < -100 or value > 100:
            raise ValueError(f"suspicious spread value: {value}")
        return float(value)


# ============================================================
# Safe HTTP client
# ============================================================

class HttpClient:
    def __init__(self, min_delay_seconds: float = 0.25, timeout_seconds: int = 30) -> None:
        self.session = requests.Session()
        self.min_delay_seconds = min_delay_seconds
        self.timeout_seconds = timeout_seconds
        self.last_request_ts = 0.0
        self.session.headers.update({
            "User-Agent": "rates-curves-spreads-research-scraper/1.0 (contact: local-research)",
            "Accept": "application/json,text/csv,text/html;q=0.9,*/*;q=0.8",
        })

    def _throttle(self) -> None:
        elapsed = time.time() - self.last_request_ts
        wait = max(0.0, self.min_delay_seconds - elapsed)
        if wait > 0:
            time.sleep(wait)

    @retry(
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError, requests.HTTPError)),
        wait=wait_exponential_jitter(initial=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        self._throttle()
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        self.last_request_ts = time.time()
        if response.status_code in {429, 500, 502, 503, 504}:
            LOGGER.warning("http_retryable_status", url=response.url, status_code=response.status_code)
            response.raise_for_status()
        if response.status_code >= 400:
            LOGGER.error("http_bad_status", url=response.url, status_code=response.status_code, text=response.text[:500])
            response.raise_for_status()
        return response


HTTP = HttpClient()


# ============================================================
# Safe file writer
# ============================================================

class FileWriter:
    @staticmethod
    def checksum_bytes(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def atomic_write_bytes(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        tmp.write_bytes(data)
        tmp.replace(path)

    @staticmethod
    def atomic_write_json(path: Path, payload: Any) -> None:
        data = json.dumps(payload, default=str, ensure_ascii=False, indent=2).encode("utf-8")
        FileWriter.atomic_write_bytes(path, data)

    @staticmethod
    def write_dataframe_csv(path: Path, df: pd.DataFrame) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        df.to_csv(tmp, index=False, quoting=csv.QUOTE_MINIMAL)
        tmp.replace(path)


# ============================================================
# MongoDB writer
# ============================================================

class MongoWriter:
    def __init__(self, mongo_uri: str, db_name: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        self.db.rate_observations.create_index([("series_id", 1), ("date", 1)], unique=True)
        self.db.curve_snapshots.create_index([("curve_id", 1), ("date", 1)], unique=True)
        self.db.spread_observations.create_index([("spread_id", 1), ("date", 1)], unique=True)
        self.db.ingestion_runs.create_index([("run_id", 1)], unique=True)

    @staticmethod
    def _to_mongo_doc(model: BaseModel) -> Dict[str, Any]:
        doc = model.model_dump()
        # Mongo supports datetime but not date cleanly; store date as ISO string for predictable querying.
        if isinstance(doc.get("date"), date):
            doc["date"] = doc["date"].isoformat()
        return doc

    def upsert_rates(self, observations: Sequence[RateObservation]) -> int:
        return self._bulk_upsert(
            self.db.rate_observations,
            observations,
            key_fields=["series_id", "date"],
        )

    def upsert_curves(self, curves: Sequence[CurveSnapshot]) -> int:
        return self._bulk_upsert(
            self.db.curve_snapshots,
            curves,
            key_fields=["curve_id", "date"],
        )

    def upsert_spreads(self, spreads: Sequence[SpreadObservation]) -> int:
        return self._bulk_upsert(
            self.db.spread_observations,
            spreads,
            key_fields=["spread_id", "date"],
        )

    def _bulk_upsert(self, collection: Collection, models: Sequence[BaseModel], key_fields: Sequence[str]) -> int:
        if not models:
            return 0
        ops = []
        for model in models:
            doc = self._to_mongo_doc(model)
            key = {field: doc[field] for field in key_fields}
            ops.append(UpdateOne(key, {"$set": doc}, upsert=True))
        result = collection.bulk_write(ops, ordered=False)
        return int(result.upserted_count + result.modified_count + result.matched_count)

    def insert_run(self, run_doc: Dict[str, Any]) -> None:
        self.db.ingestion_runs.update_one({"run_id": run_doc["run_id"]}, {"$set": run_doc}, upsert=True)


# ============================================================
# Parsing utilities
# ============================================================

class SafeParser:
    @staticmethod
    def parse_date(value: Any) -> Optional[date]:
        if value is None or pd.isna(value):
            return None
        try:
            return pd.to_datetime(value, errors="raise").date()
        except Exception:
            return None

    @staticmethod
    def parse_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if value in {"", ".", "NA", "N/A", "null", "None"}:
                return None
            value = value.replace(",", "")
        try:
            f = float(value)
        except Exception:
            return None
        if pd.isna(f):
            return None
        return f

    @staticmethod
    def require_columns(df: pd.DataFrame, required: Sequence[str], context: str) -> None:
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"{context}: missing required columns: {missing}; actual={list(df.columns)}")


# ============================================================
# Source adapters
# ============================================================

class FredClient:
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key: Optional[str]) -> None:
        self.api_key = api_key

    def fetch_series(self, spec: SeriesSpec, start: str, end: str) -> List[RateObservation]:
        if not self.api_key:
            LOGGER.warning("fred_skipped_missing_api_key", series_id=spec.series_id)
            return []

        params = {
            "series_id": spec.series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start,
            "observation_end": end,
        }
        response = HTTP.get(self.BASE_URL, params=params)
        raw_bytes = response.content
        raw_path = RAW_DIR / "fred" / spec.series_id / f"{start}_{end}.json"
        FileWriter.atomic_write_bytes(raw_path, raw_bytes)

        payload = response.json()
        observations = payload.get("observations", [])
        if not isinstance(observations, list):
            raise ValueError(f"FRED bad payload for {spec.series_id}: observations is not a list")

        out: List[RateObservation] = []
        rejected = 0
        for row in observations:
            d = SafeParser.parse_date(row.get("date"))
            v = SafeParser.parse_float(row.get("value"))
            if d is None or v is None:
                rejected += 1
                continue
            try:
                out.append(RateObservation(
                    series_id=spec.series_id,
                    source=spec.source,
                    date=d,
                    value=v,
                    unit=spec.unit,
                    frequency=spec.frequency,
                    category=spec.category,
                    description=spec.description,
                ))
            except ValidationError as exc:
                rejected += 1
                LOGGER.warning("fred_validation_rejected", series_id=spec.series_id, date=str(d), error=str(exc))

        LOGGER.info("fred_series_fetched", series_id=spec.series_id, rows=len(out), rejected=rejected, raw_path=str(raw_path))
        return out


class TreasuryFiscalDataClient:
    """
    Uses the FiscalData API pattern. The endpoint name is intentionally isolated here because
    Treasury occasionally changes table paths or field names. If it breaks, this class is the
    only part to modify.
    """

    BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/daily_treasury_rates"
    PAGE_SIZE = 10000
    MAX_PAGES = 500

    def fetch_daily_par_yield_curve(self, start: str, end: str) -> List[CurveSnapshot]:
        fields = ["record_date", *TREASURY_CURVE_FIELDS.keys()]
        params_base = {
            "fields": ",".join(fields),
            "filter": f"record_date:gte:{start},record_date:lte:{end}",
            "sort": "record_date",
            "page[size]": self.PAGE_SIZE,
            "format": "json",
        }

        all_rows: List[Dict[str, Any]] = []
        page = 1
        while True:
            if page > self.MAX_PAGES:
                raise RuntimeError(f"Treasury pagination exceeded hard cap: {self.MAX_PAGES}")
            params = dict(params_base)
            params["page[number]"] = page
            response = HTTP.get(self.BASE_URL, params=params)
            raw_path = RAW_DIR / "treasury" / "daily_treasury_rates" / f"{start}_{end}_page_{page:04d}.json"
            FileWriter.atomic_write_bytes(raw_path, response.content)
            payload = response.json()
            rows = payload.get("data", [])
            if not isinstance(rows, list):
                raise ValueError("Treasury bad payload: data is not a list")
            all_rows.extend(rows)

            meta = payload.get("meta", {})
            total_pages = int(meta.get("total-pages", page)) if meta.get("total-pages") else page
            LOGGER.info("treasury_page_fetched", page=page, total_pages=total_pages, rows=len(rows))
            if page >= total_pages or not rows:
                break
            page += 1

        curves: List[CurveSnapshot] = []
        rejected = 0
        for row in all_rows:
            d = SafeParser.parse_date(row.get("record_date"))
            if d is None:
                rejected += 1
                continue
            points: List[CurvePoint] = []
            for field, years in sorted(TREASURY_CURVE_FIELDS.items(), key=lambda kv: kv[1]):
                value = SafeParser.parse_float(row.get(field))
                if value is None:
                    continue
                try:
                    points.append(CurvePoint(
                        tenor=TREASURY_TENOR_LABELS[field],
                        years=years,
                        rate=value,
                    ))
                except ValidationError as exc:
                    LOGGER.warning("treasury_point_rejected", date=str(d), field=field, value=row.get(field), error=str(exc))

            try:
                curves.append(CurveSnapshot(
                    curve_id="UST_PAR_NOMINAL",
                    source="US_TREASURY_FISCALDATA",
                    date=d,
                    points=points,
                ))
            except ValidationError as exc:
                rejected += 1
                LOGGER.warning("treasury_curve_rejected", date=str(d), error=str(exc))

        df = pd.DataFrame([c.model_dump() for c in curves])
        if not df.empty:
            FileWriter.write_dataframe_csv(PROCESSED_DIR / "treasury_curve_snapshots.csv", df)
        LOGGER.info("treasury_curves_fetched", rows=len(curves), rejected=rejected)
        return curves


class NewYorkFedClient:
    """
    NY Fed Markets API endpoints use structured REST URLs and can return JSON/CSV.
    Endpoint shapes differ by dataset. Keep source-specific logic isolated here.

    This implementation uses a conservative endpoint dictionary. If the NY Fed endpoint
    changes, the safeguard is to fail loudly and keep raw response files for inspection.
    """

    BASE_URL = "https://markets.newyorkfed.org/api"

    ENDPOINTS = {
        # Common endpoint shape for reference rates in the Markets API.
        # The code validates the returned JSON before trusting it.
        "SOFR": "/rates/secured/sofr/search.json",
        "EFFR": "/rates/unsecured/effr/search.json",
        "OBFR": "/rates/unsecured/obfr/search.json",
        "TGCR": "/rates/secured/tgcr/search.json",
        "BGCR": "/rates/secured/bgcr/search.json",
    }

    def fetch_reference_rate(self, rate_name: str, start: str, end: str) -> List[RateObservation]:
        endpoint = self.ENDPOINTS[rate_name]
        url = self.BASE_URL + endpoint
        params = {
            "startDate": start,
            "endDate": end,
            "type": "rate",
        }
        response = HTTP.get(url, params=params)
        raw_path = RAW_DIR / "nyfed" / rate_name / f"{start}_{end}.json"
        FileWriter.atomic_write_bytes(raw_path, response.content)

        try:
            payload = response.json()
        except Exception as exc:
            LOGGER.error("nyfed_json_parse_failed", rate_name=rate_name, text=response.text[:500], error=str(exc))
            return []

        candidate_rows = self._extract_rows(payload)
        observations: List[RateObservation] = []
        rejected = 0

        for row in candidate_rows:
            d = self._extract_date(row)
            v = self._extract_rate_value(row)
            if d is None or v is None:
                rejected += 1
                continue
            try:
                observations.append(RateObservation(
                    series_id=rate_name,
                    source="NEW_YORK_FED_MARKETS_API",
                    date=d,
                    value=v,
                    unit="percent",
                    frequency="daily",
                    category="money_market",
                    description=f"{rate_name} reference rate",
                ))
            except ValidationError as exc:
                rejected += 1
                LOGGER.warning("nyfed_validation_rejected", rate_name=rate_name, date=str(d), error=str(exc))

        LOGGER.info("nyfed_rate_fetched", rate_name=rate_name, rows=len(observations), rejected=rejected, raw_path=str(raw_path))
        return observations

    @staticmethod
    def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ["refRates", "data", "rates", "observations"]:
            rows = payload.get(key)
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
        # Fallback: search one level deep for the first list of dicts.
        for value in payload.values():
            if isinstance(value, list) and all(isinstance(x, dict) for x in value[:5]):
                return value
        return []

    @staticmethod
    def _extract_date(row: Dict[str, Any]) -> Optional[date]:
        for key in ["effectiveDate", "date", "recordDate", "asOfDate"]:
            d = SafeParser.parse_date(row.get(key))
            if d is not None:
                return d
        return None

    @staticmethod
    def _extract_rate_value(row: Dict[str, Any]) -> Optional[float]:
        for key in ["percentRate", "rate", "value", "ratePercent"]:
            v = SafeParser.parse_float(row.get(key))
            if v is not None:
                return v
        return None


# ============================================================
# Derived spread builder
# ============================================================

class SpreadBuilder:
    @staticmethod
    def from_rate_observations(rate_obs: Sequence[RateObservation]) -> List[SpreadObservation]:
        df = pd.DataFrame([x.model_dump() for x in rate_obs])
        if df.empty:
            return []
        df["date"] = pd.to_datetime(df["date"]).dt.date
        pivot = df.pivot_table(index="date", columns="series_id", values="value", aggfunc="last")

        definitions = {
            "UST_10Y_2Y_DERIVED": ("DGS10", "DGS2"),
            "UST_10Y_3M_DERIVED": ("DGS10", "DGS3MO"),
            "UST_5Y_2Y_DERIVED": ("DGS5", "DGS2"),
            "UST_30Y_10Y_DERIVED": ("DGS30", "DGS10"),
        }

        out: List[SpreadObservation] = []
        for spread_id, (long_leg, short_leg) in definitions.items():
            if long_leg not in pivot.columns or short_leg not in pivot.columns:
                continue
            series = pivot[long_leg] - pivot[short_leg]
            for d, v in series.dropna().items():
                try:
                    out.append(SpreadObservation(
                        spread_id=spread_id,
                        source="DERIVED_FROM_RATE_OBSERVATIONS",
                        date=d,
                        value=float(v),
                        legs={"long_leg": long_leg, "short_leg": short_leg},
                    ))
                except ValidationError as exc:
                    LOGGER.warning("derived_spread_rejected", spread_id=spread_id, date=str(d), error=str(exc))
        LOGGER.info("derived_spreads_built", rows=len(out))
        return out


# ============================================================
# Pipeline
# ============================================================

class IngestionPipeline:
    def __init__(self, mongo_writer: MongoWriter, fred_api_key: Optional[str]) -> None:
        self.mongo = mongo_writer
        self.fred = FredClient(fred_api_key)
        self.treasury = TreasuryFiscalDataClient()
        self.nyfed = NewYorkFedClient()

    def run(self, start: str, end: str, include_fred: bool, include_treasury: bool, include_nyfed: bool) -> None:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
        started_at = datetime.now(timezone.utc)
        LOGGER.info("ingestion_started", run_id=run_id, start=start, end=end)

        total_inserted = 0
        total_failed = 0
        sources: List[str] = []

        try:
            fred_rates: List[RateObservation] = []
            if include_fred:
                sources.append("FRED")
                for spec in FRED_SERIES:
                    try:
                        obs = self.fred.fetch_series(spec, start, end)
                        fred_rates.extend(obs)
                    except Exception as exc:
                        total_failed += 1
                        LOGGER.error("fred_series_failed", series_id=spec.series_id, error=str(exc))
                inserted = self.mongo.upsert_rates(fred_rates)
                total_inserted += inserted
                LOGGER.info("fred_upsert_done", rows=len(fred_rates), inserted_or_matched=inserted)

                derived = SpreadBuilder.from_rate_observations(fred_rates)
                inserted_spreads = self.mongo.upsert_spreads(derived)
                total_inserted += inserted_spreads
                LOGGER.info("derived_spreads_upsert_done", rows=len(derived), inserted_or_matched=inserted_spreads)

            if include_treasury:
                sources.append("US_TREASURY_FISCALDATA")
                try:
                    curves = self.treasury.fetch_daily_par_yield_curve(start, end)
                    inserted = self.mongo.upsert_curves(curves)
                    total_inserted += inserted
                    LOGGER.info("treasury_upsert_done", rows=len(curves), inserted_or_matched=inserted)
                except Exception as exc:
                    total_failed += 1
                    LOGGER.error("treasury_failed", error=str(exc))

            if include_nyfed:
                sources.append("NEW_YORK_FED_MARKETS_API")
                nyfed_rates: List[RateObservation] = []
                for rate_name in ["SOFR", "EFFR", "OBFR", "TGCR", "BGCR"]:
                    try:
                        nyfed_rates.extend(self.nyfed.fetch_reference_rate(rate_name, start, end))
                    except Exception as exc:
                        total_failed += 1
                        LOGGER.error("nyfed_rate_failed", rate_name=rate_name, error=str(exc))
                inserted = self.mongo.upsert_rates(nyfed_rates)
                total_inserted += inserted
                LOGGER.info("nyfed_upsert_done", rows=len(nyfed_rates), inserted_or_matched=inserted)

            status = "success" if total_failed == 0 else "partial_failure"
        except Exception as exc:
            status = "failure"
            total_failed += 1
            LOGGER.error("ingestion_fatal", run_id=run_id, error=str(exc))
            raise
        finally:
            ended_at = datetime.now(timezone.utc)
            run_doc = {
                "run_id": run_id,
                "started_at": started_at,
                "ended_at": ended_at,
                "status": status,
                "records_inserted_or_matched": total_inserted,
                "failure_count": total_failed,
                "sources": sources,
                "start": start,
                "end": end,
            }
            self.mongo.insert_run(run_doc)
            LOGGER.info("ingestion_finished", **run_doc)


# ============================================================
# Data reading examples / safeguards
# ============================================================

class DataReader:
    """
    Safe readers for downstream research. These functions read from MongoDB and enforce
    shape assumptions before returning pandas DataFrames.
    """

    def __init__(self, mongo_writer: MongoWriter) -> None:
        self.mongo = mongo_writer

    def read_rate_series(self, series_id: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        query: Dict[str, Any] = {"series_id": series_id}
        if start or end:
            date_filter: Dict[str, str] = {}
            if start:
                date_filter["$gte"] = start
            if end:
                date_filter["$lte"] = end
            query["date"] = date_filter
        rows = list(self.mongo.db.rate_observations.find(query, {"_id": 0}).sort("date", 1))
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(columns=["date", "value"])
        SafeParser.require_columns(df, ["date", "value", "series_id"], f"rate series {series_id}")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values("date")
        df = df.drop_duplicates(subset=["date"], keep="last")
        return df[["date", "value", "series_id", "source", "unit", "category"]]

    def read_curve_snapshots(self, curve_id: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        query: Dict[str, Any] = {"curve_id": curve_id}
        if start or end:
            date_filter: Dict[str, str] = {}
            if start:
                date_filter["$gte"] = start
            if end:
                date_filter["$lte"] = end
            query["date"] = date_filter
        rows = list(self.mongo.db.curve_snapshots.find(query, {"_id": 0}).sort("date", 1))
        flat_rows: List[Dict[str, Any]] = []
        for row in rows:
            for point in row.get("points", []):
                flat_rows.append({
                    "date": row.get("date"),
                    "curve_id": row.get("curve_id"),
                    "source": row.get("source"),
                    "curve_type": row.get("curve_type"),
                    "currency": row.get("currency"),
                    "tenor": point.get("tenor"),
                    "years": point.get("years"),
                    "rate": point.get("rate"),
                })
        df = pd.DataFrame(flat_rows)
        if df.empty:
            return pd.DataFrame(columns=["date", "tenor", "years", "rate"])
        SafeParser.require_columns(df, ["date", "tenor", "years", "rate"], f"curve {curve_id}")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["years"] = pd.to_numeric(df["years"], errors="coerce")
        df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
        df = df.dropna(subset=["date", "years", "rate"]).sort_values(["date", "years"])
        return df

    def curve_matrix(self, curve_id: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        df = self.read_curve_snapshots(curve_id, start, end)
        if df.empty:
            return df
        matrix = df.pivot_table(index="date", columns="tenor", values="rate", aggfunc="last")
        tenor_order = sorted(matrix.columns, key=lambda t: df.loc[df["tenor"] == t, "years"].iloc[0])
        return matrix[tenor_order]


# ============================================================
# CLI
# ============================================================

def valid_date_string(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape/ingest rates, curves, and spreads safely.")
    parser.add_argument("--start", type=valid_date_string, required=True)
    parser.add_argument("--end", type=valid_date_string, required=True)
    parser.add_argument("--skip-fred", action="store_true")
    parser.add_argument("--skip-treasury", action="store_true")
    parser.add_argument("--skip-nyfed", action="store_true")
    args = parser.parse_args()

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "rates_curves_spreads")
    fred_api_key = os.getenv("FRED_API_KEY")

    if args.start > args.end:
        raise SystemExit("--start cannot be later than --end")

    mongo = MongoWriter(mongo_uri, mongo_db)
    pipeline = IngestionPipeline(mongo, fred_api_key)
    pipeline.run(
        start=args.start,
        end=args.end,
        include_fred=not args.skip_fred,
        include_treasury=not args.skip_treasury,
        include_nyfed=not args.skip_nyfed,
    )

    # Example read-back smoke test.
    reader = DataReader(mongo)
    dgs10 = reader.read_rate_series("DGS10", args.start, args.end)
    print(f"DGS10 rows read back: {len(dgs10)}")
    curve = reader.curve_matrix("UST_PAR_NOMINAL", args.start, args.end)
    print(f"UST_PAR_NOMINAL curve matrix shape: {curve.shape}")


if __name__ == "__main__":
    main()
