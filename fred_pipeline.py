"""
FRED (Federal Reserve Economic Data) pipeline.

Extracts economic time series from the FRED REST API, loads them incrementally
into DuckDB using dlt, and is designed to run on a daily/weekly schedule via
GitHub Actions.

Each series gets its own resource with its own independent incremental cursor,
so one series's cursor never leaks into another's start date. All resources
write into the same destination table via table_name.
"""

import time
import functools
import dlt
import requests

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SERIES_IDS = [
    "UNRATE",      # Unemployment rate
    "CPIAUCSL",    # CPI, inflation
    "FEDFUNDS",    # Federal funds rate
    "INDPRO",      # Industrial production index (manufacturing signal)
    "RSXFS",       # Retail sales
]


def retry_on_failure(max_retries=3, base_wait=30):
    """Retry decorator with exponential backoff for transient API/network errors."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException,) as e:
                    attempt += 1
                    if attempt > max_retries:
                        raise
                    wait = base_wait * (2 ** (attempt - 1))
                    print(f"Request failed ({e}). Retrying in {wait}s "
                          f"(attempt {attempt}/{max_retries})...")
                    time.sleep(wait)
        return wrapper
    return decorator


@retry_on_failure()
def fetch_series_observations(series_id: str, api_key: str, start_date: str):
    """Fetch observations for one FRED series, from start_date onward."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
    }
    resp = requests.get(FRED_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("observations", [])


def make_series_resource(series_id: str):
    """
    Build a dlt resource for a single FRED series.

    Each resource has its own name (so dlt tracks a separate incremental
    cursor per series) but shares the same table_name, so all series end up
    in one merged fred_observations table.
    """

    @dlt.resource(
        name=f"fred_{series_id.lower()}",
        table_name="fred_observations",
        write_disposition="merge",
        primary_key=["series_id", "date"],
    )
    def series_resource(
        api_key: str = dlt.secrets.value,
        updated_at=dlt.sources.incremental("date", initial_value="1900-01-01"),
    ):
        start_date = updated_at.last_value or "1900-01-01"
        observations = fetch_series_observations(series_id, api_key, start_date)

        for obs in observations:
            if obs.get("value") == ".":
                continue
            yield {
                "series_id": series_id,
                "date": obs["date"],
                "value": float(obs["value"]),
                "realtime_start": obs.get("realtime_start"),
                "realtime_end": obs.get("realtime_end"),
            }

    return series_resource


@dlt.source
def fred_source():
    for series_id in SERIES_IDS:
        yield make_series_resource(series_id)


def load_fred_economic_data():
    pipeline = dlt.pipeline(
        pipeline_name="fred_pipeline",
        destination="duckdb",
        dataset_name="fred_economic_data",
        dev_mode=False,
    )
    load_info = pipeline.run(fred_source())
    print(load_info)


if __name__ == "__main__":
    load_fred_economic_data()