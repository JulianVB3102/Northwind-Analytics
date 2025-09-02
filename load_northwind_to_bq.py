import os
import sqlite3
from typing import List

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# =========  SETTINGS  =========
GCP_PROJECT_ID   = "northwind-analytics-470720"
BQ_DATASET_ID    = "northwind"          # dataset to create/use
SQLITE_DB_PATH = "C:\\Users\\Julian\\Northwind\\Northwind.db"       # path to your SQLite file
LOCATION         = "US"                  # BQ dataset location
WRITE_DISPOSITION = "WRITE_TRUNCATE"     # or "WRITE_APPEND"
INCLUDE_TABLES: List[str] = []           # e.g. ["Orders", "OrderDetails"]; empty = all
EXCLUDE_TABLES: List[str] = []           # e.g. ["sqlite_sequence"]
# =============================================

def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str = "US"):
    ds_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    try:
        client.get_dataset(ds_ref)
        print(f"Dataset {dataset_id} exists.")
    except NotFound:
        ds_ref.location = location
        client.create_dataset(ds_ref)
        print(f"Created dataset {dataset_id} in {location}.")

def list_sqlite_tables(sqlite_path: str) -> List[str]:
    con = sqlite3.connect(sqlite_path)
    try:
        q = """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
        tables = [r[0] for r in con.execute(q).fetchall()]
        return tables
    finally:
        con.close()

def load_table_to_bq(df: pd.DataFrame, table_name: str, client: bigquery.Client, dataset_id: str):
    # Normalize column names: BigQuery likes alphanumerics + underscores
    df.columns = [c.strip().replace(" ", "_").replace("-", "_") for c in df.columns]

    table_id = f"{client.project}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=WRITE_DISPOSITION,
        autodetect=True,
    )
    # Use Arrow path for best schema handling
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # wait for job
    dest = client.get_table(table_id)
    print(f"Loaded {len(df):,} rows into {table_id} (schema has {len(dest.schema)} columns).")

def main():
    # BigQuery client (uses ADC: gcloud or service account env var)
    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Ensure dataset
    ensure_dataset(client, BQ_DATASET_ID, LOCATION)

    # Which tables to move
    tables = INCLUDE_TABLES or list_sqlite_tables(SQLITE_DB_PATH)
    tables = [t for t in tables if t not in EXCLUDE_TABLES]
    if not tables:
        raise SystemExit("No tables found to load.")

    # Open SQLite once
    con = sqlite3.connect(SQLITE_DB_PATH)

    try:
        for t in tables:
            # Read whole table (Northwind sizes are small)
            df = pd.read_sql_query(f"SELECT * FROM [{t}]", con)

            # Coerce obvious date columns (helps BigQuery types)
            for col in df.columns:
                if "date" in col.lower():
                    # errors='ignore' keeps non-date text intact
                    df[col] = pd.to_datetime(df[col], errors="ignore")

            # Table names: BigQuery must start with a letter or underscore; keep simple
            safe_t = t
            if not (safe_t[0].isalpha() or safe_t[0] == "_"):
                safe_t = f"t_{safe_t}"
            safe_t = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in safe_t)

            load_table_to_bq(df, safe_t, client, BQ_DATASET_ID)

    finally:
        con.close()

if __name__ == "__main__":
    main()
