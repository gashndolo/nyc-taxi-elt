import argparse
import logging

from ingestion import tlc, weather

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main():
    p = argparse.ArgumentParser(description="Ingest one month of taxi + weather data")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--force", action="store_true", help="overwrite existing files")
    args = p.parse_args()

    tlc.ingest(args.year, args.month, args.force)
    weather.ingest(args.year, args.month, args.force)


if __name__ == "__main__":
    main()