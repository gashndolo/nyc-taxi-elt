import argparse
import logging

from ingestion import tlc, weather, bq
from ingestion.tables import TAXI, WEATHER

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main():
    p = argparse.ArgumentParser(description="Ingest and load one month of data")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--force", action="store_true", help="re-download to GCS")
    p.add_argument("--skip-load", action="store_true", help="GCS only, no BigQuery")
    args = p.parse_args()

    taxi_path = tlc.ingest(args.year, args.month, args.force)
    weather_path = weather.ingest(args.year, args.month, args.force)

    if not args.skip_load:
        bq.load_month(TAXI, args.year, args.month, taxi_path)
        bq.load_month(WEATHER, args.year, args.month, weather_path)


if __name__ == "__main__":
    main()