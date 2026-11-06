
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BUCKET = os.environ["GCS_BUCKET"]

TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


# Open-Meteo historical weather (free, no API key)
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
NYC_LAT, NYC_LON = 40.7128, -74.0060
WEATHER_VARS = [
    "temperature_2m", "precipitation", "rain", "snowfall",
    "wind_speed_10m", "weather_code",
]

REGION = os.environ.get("GCP_REGION", "us-central1")
RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "raw")