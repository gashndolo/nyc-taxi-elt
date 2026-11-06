import calendar
import logging
import tempfile
import pandas as pd
import requests

from ingestion import gcs
from ingestion.config import WEATHER_URL, NYC_LAT, NYC_LON, WEATHER_VARS

log = logging.getLogger(__name__)


def gcs_path(year: int, month: int) -> str:
    return (f"raw/weather/year={year}/month={month:02d}/"
            f"weather_{year}-{month:02d}.parquet")


def ingest(year: int, month: int, force: bool = False) -> str:
    path = gcs_path(year, month)
    if gcs.exists(path) and not force:
        log.info("Skip (already exists): %s", path)
        return path

    last_day = calendar.monthrange(year, month)[1]
    params = {
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": f"{year}-{month:02d}-01",
        "end_date": f"{year}-{month:02d}-{last_day}",
        "hourly": ",".join(WEATHER_VARS),
        "timezone": "America/New_York",
    }
    r = requests.get(WEATHER_URL, params=params, timeout=60)
    r.raise_for_status()
    hourly = r.json()["hourly"]

    df = pd.DataFrame(hourly)
    df["time"] = pd.to_datetime(df["time"])  # local NYC time, matches TLC pickups

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        df.to_parquet(tmp.name, index=False)
        gcs.upload_file(tmp.name, path)

    log.info("Uploaded %d hourly rows: %s", len(df), path)
    return path