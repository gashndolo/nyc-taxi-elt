from google.cloud import bigquery

bq = bigquery.Client(project="nyc-taxi-elt", location="us-central1")

QUERY = """
SELECT COUNT(*) AS trips, AVG(tip_amount) AS avg_tip
FROM `nyc-taxi-elt.raw.{table}`
WHERE tpep_pickup_datetime >= DATETIME '2024-02-10 00:00:00'
  AND tpep_pickup_datetime <  DATETIME '2024-02-17 00:00:00'
  AND pulocationid = 237
"""

for table in ["taxi_trips_flat", "taxi_trips"]:
    job = bq.query(QUERY.format(table=table),
                   job_config=bigquery.QueryJobConfig(use_query_cache=False))
    job.result()
    print(f"{table:18} processed={job.total_bytes_processed/1e6:8.1f} MB  "
          f"billed={job.total_bytes_billed/1e6:8.1f} MB")