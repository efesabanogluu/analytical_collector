from google.cloud import storage, bigquery
import os
import tempfile
from datetime import datetime, timezone, timedelta
import pandas as pd
from google.cloud import storage, bigquery
import io
from datetime import datetime


def process_files(request):
    DATASET_ID = os.getenv('BIGQUERY_DATASET')
    RAW_TABLE_ID = os.getenv('BIGQUERY_TABLE')
    PROJECT_ID = os.getenv('PROJECT_ID')
    CONTROL_TABLE_ID = os.getenv('CONTROL_TABLE_ID')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    FOLDER_PATH = os.getenv('FOLDER_PATH')

    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT_ID)

    raw_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE_ID}"
    control_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{CONTROL_TABLE_ID}"

    query = f"SELECT file_name FROM `{control_table_ref}`"
    processed_files = set(row.file_name for row in bq_client.query(query).result())

    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=FOLDER_PATH)
    new_files = []

    for blob in blobs:
        file_name = blob.name
        if file_name.endswith('.parquet') and file_name not in processed_files:
            print(f"Processing new file: {file_name}")

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("eventTime", "TIMESTAMP"),
                    bigquery.SchemaField("processTime", "TIMESTAMP"),
                    bigquery.SchemaField("user", "INT64"),
                    bigquery.SchemaField("level", "INT64"),
                    bigquery.SchemaField("state", "STRING"),
                ]
            )

            # Dosya URI’si (gs://...)
            source_uri = f"gs://{BUCKET_NAME}/{file_name}"

            load_job = bq_client.load_table_from_uri(
                source_uri,
                raw_table_ref,
                job_config=job_config
            )
            load_job.result()  # İş bitene kadar bekle

            rows_to_insert = [{
                'file_name': file_name,
                'processed_at': datetime.utcnow().isoformat()
            }]
            bq_client.insert_rows_json(control_table_ref, rows_to_insert)

            print(f"Finished processing: {file_name}")
            new_files.append(file_name)

    return f"Processed {len(new_files)} new files."
