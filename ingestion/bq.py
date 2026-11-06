import logging
from datetime import date

from google.cloud import bigquery

from ingestion.config import PROJECT_ID, BUCKET, REGION, RAW_DATASET

log = logging.getLogger(__name__)

def client() -> bigquery.client:
    return bigquery.Client(project=PROJECT_ID, location=REGION)

def _month_bounds(year: int, month: int):
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1)
    return start, end

def _create_table_if_missing(bq, spec, target):
    cols = ",\n  ".join(f"`{n}` {t}" for n, t in spec["columns"])
    bq.query(f"""
        CREATE TABLE IF NOT EXISTS {target} (
          {cols},
          _source_file STRING,
          _loaded_at TIMESTAMP
        ) {spec['options']}
    """).result()

def load_month(spec: dict, year: int, month: int, gcs_path: str) -> int:
    bq = client()
    target = f"`{PROJECT_ID}.{RAW_DATASET}.{spec['table']}`"
    stg_id = f"{PROJECT_ID}.{RAW_DATASET}.tmp_{spec['table']}_{year}_{month:02d}"
    uri = f"gs://{BUCKET}/{gcs_path}"
    start, end = _month_bounds(year, month)

    _create_table_if_missing(bq, spec, target)

    try:
        # 1. Load the raw Parquet into a temporary staging table
        job = bq.load_table_from_uri(
            uri, stg_id,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
            ),
        )
        job.result()
        stg = bq.get_table(stg_id)
        staged = stg.num_rows

        # 2. Match source columns to our contract, ignoring capitalization
        src = {f.name.lower(): f.name for f in stg.schema}
        wanted = {n for n, _ in spec["columns"]}
        for missing in sorted(wanted - src.keys()):
            log.warning("Column missing in source, filled with NULL: %s", missing)
        for extra in sorted(src.keys() - wanted):
            log.warning("New source column ignored (schema drift?): %s", extra)

        exprs = []
        for name, typ in spec["columns"]:
            if name in src:
                exprs.append(f"SAFE_CAST(`{src[name]}` AS {typ}) AS `{name}`")
            else:
                exprs.append(f"CAST(NULL AS {typ}) AS `{name}`")

        # 3. Replace this month's rows atomically
        col_list = ", ".join(f"`{n}`" for n, _ in spec["columns"])
        f = f"`{spec['month_filter']}`"
        bounds = (f"{f} >= DATETIME '{start} 00:00:00' "
                  f"AND {f} < DATETIME '{end} 00:00:00'")
        bq.query(f"""
            BEGIN TRANSACTION;
            DELETE FROM {target} WHERE {bounds};
            INSERT INTO {target} ({col_list}, _source_file, _loaded_at)
            SELECT {col_list}, '{uri}', CURRENT_TIMESTAMP()
            FROM (SELECT {', '.join(exprs)} FROM `{stg_id}`)
            WHERE {bounds};
            COMMIT TRANSACTION;
        """).result()

        kept = list(bq.query(
            f"SELECT COUNT(*) AS n FROM {target} WHERE {bounds}").result())[0].n
        log.info("%s %d-%02d: staged=%d kept=%d dropped_out_of_range=%d",
                 spec["table"], year, month, staged, kept, staged - kept)
        return kept
    finally:
        bq.delete_table(stg_id, not_found_ok=True)