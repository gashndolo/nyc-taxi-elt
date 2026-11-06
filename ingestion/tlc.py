
import logging
import tempfile
import requests

from ingestion import gcs
from ingestion.config import TLC_URL

log = logging.getLogger(__name__)

def gcs_path(year: int, month: int) -> str:
    return (f"raw/taxi/year={year}/month={month:02d}/"
            f"yellow_tripdata_{year}-{month:02d}.parquet")

def ingest(year: int, month: int, force: bool = False) -> str:
    path = gcs_path(year, month)
    if gcs.exists(path) and not force:
        log.info("Skip (already exists): %s", path)
        return path

    url = TLC_URL.format(year=year, month=month)
    log.info("Downloading %s", url)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                tmp.write(chunk)
        tmp.flush()
        gcs.upload_file(tmp.name, path)

    log.info("Uploaded: %s", path)
    return path