from datetime import date

import pytest

from ingestion import bq, tlc, weather
from ingestion.tables import TAXI, WEATHER


def test_tlc_path_is_deterministic():
    assert tlc.gcs_path(2024, 1) == (
        "raw/taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet")


def test_weather_path_is_deterministic():
    assert weather.gcs_path(2024, 11) == (
        "raw/weather/year=2024/month=11/weather_2024-11.parquet")


def test_month_bounds_regular_month():
    assert bq._month_bounds(2024, 2) == (date(2024, 2, 1), date(2024, 3, 1))


def test_month_bounds_december_rolls_over_year():
    assert bq._month_bounds(2024, 12) == (date(2024, 12, 1), date(2025, 1, 1))


@pytest.mark.parametrize("spec", [TAXI, WEATHER])
def test_schema_contract_is_consistent(spec):
    names = [name for name, _ in spec["columns"]]
    assert len(names) == len(set(names)), "duplicate column in contract"
    assert spec["month_filter"] in names


@pytest.mark.parametrize("module", [tlc, weather])
def test_ingest_skips_existing_file(monkeypatch, module):
    """Idempotency: if the object exists, nothing is downloaded."""
    monkeypatch.setattr(module.gcs, "exists", lambda path: True)

    def no_download(*args, **kwargs):
        raise AssertionError("download attempted for an existing file")

    monkeypatch.setattr(module.requests, "get", no_download)
    assert module.ingest(2024, 1) == module.gcs_path(2024, 1)