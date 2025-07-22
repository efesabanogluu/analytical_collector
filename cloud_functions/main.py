from google.cloud import storage, bigquery
import os
import tempfile
from datetime import datetime, timezone, timedelta
import pandas as pd
from google.cloud import storage, bigquery
import pyarrow.parquet as pq
import io
from datetime import datetime
# def gcs_to_bigquery(event, context):
#     """
#     Triggered by a change to a Cloud Storage bucket.
#     Args:
#         event (dict): Event payload.
#         context (google.cloud.functions.Context): Metadata for the event.
#     """
#     bucket_name = event['bucket']
#     file_name = event['name']

#     print(f"Processing file: {file_name} from bucket: {bucket_name}")

#     # Initialize clients
#     storage_client = storage.Client()
#     bq_client = bigquery.Client()

#     # Define BigQuery details
#     dataset_id = os.getenv('BIGQUERY_DATASET')
#     table_id = os.getenv('BIGQUERY_TABLE')

#     if not dataset_id or not table_id:
#         raise Exception("Environment variables BIGQUERY_DATASET and BIGQUERY_TABLE must be set")

#     dataset_ref = bq_client.dataset(dataset_id)
#     table_ref = dataset_ref.table(table_id)

#     # Download the file locally
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(file_name)

#     with tempfile.NamedTemporaryFile() as temp_file:
#         blob.download_to_filename(temp_file.name)
#         print(f"Downloaded file to {temp_file.name}")

#         # Load job config - burası dosya tipine göre değişebilir, örnek CSV
#         job_config = bigquery.LoadJobConfig(
#             source_format=bigquery.SourceFormat.CSV,
#             skip_leading_rows=1,  # başlık varsa
#             autodetect=True,
#             write_disposition=bigquery.WriteDisposition.WRITE_APPEND
#         )

#         load_job = bq_client.load_table_from_file(
#             temp_file,
#             table_ref,
#             job_config=job_config
#         )

#         load_job.result()  # bekle bitene kadar
#         print(f"Loaded {file_name} into {dataset_id}.{table_id}")


# def gcs_to_bigquery(event, context):
#     """
#     Triggered by a change to a Cloud Storage bucket.
#     Args:
#         event (dict): Event payload.
#         context (google.cloud.functions.Context): Metadata for the event.
#     """

#     # Initialize clients
#     storage_client = storage.Client()
#     bq_client = bigquery.Client()

#     # Define BigQuery details
#     dataset_id = os.getenv('BIGQUERY_DATASET')
#     table_id = os.getenv('BIGQUERY_TABLE')

#     if not dataset_id or not table_id:
#         raise Exception("Environment variables BIGQUERY_DATASET and BIGQUERY_TABLE must be set")

#     dataset_ref = bq_client.dataset(dataset_id)
#     table_ref = dataset_ref.table(table_id)

#     # Download the file locally
#     bucket = storage_client.bucket(bucket_name)
#     blobs = list(bucket.list_blobs())
    
#     now = datetime.now(timezone.utc)
#     last_24h = now - timedelta(hours=24)

#     recent_files = []

#     for blob in blobs:
#         if blob.time_created >= last_24h:
#             recent_files.append(blob.name)

#     with tempfile.NamedTemporaryFile() as temp_file:
#         blob.download_to_filename(temp_file.name)
#         print(f"Downloaded file to {temp_file.name}")

#         # Load job config - burası dosya tipine göre değişebilir, örnek CSV
#         job_config = bigquery.LoadJobConfig(
#             source_format=bigquery.SourceFormat.CSV,
#             skip_leading_rows=1,  # başlık varsa
#             autodetect=True,
#             write_disposition=bigquery.WriteDisposition.WRITE_APPEND
#         )

#         load_job = bq_client.load_table_from_file(
#             temp_file,
#             table_ref,
#             job_config=job_config
#         )

#         load_job.result()  # bekle bitene kadar
#         print(f"Loaded {file_name} into {dataset_id}.{table_id}")
#     df_sorted = df.sort_values(by='process_time')
#     df_unique = df_sorted.drop_duplicates(subset=['event_time', 'user_id', 'level'], keep='first')


def process_files(request):
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    DATASET_ID = os.getenv('BIGQUERY_DATASET')
    RAW_TABLE_ID = os.getenv('BIGQUERY_TABLE')
    PROJECT_ID = os.getenv('PROJECT_ID')
    CONTROL_TABLE_ID = os.getenv('CONTROL_TABLE_ID')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    FOLDER_PATH =  os.getenv('FOLDER_PATH')

    # Tablolar
    raw_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE_ID}"
    control_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{CONTROL_TABLE_ID}"

    # İşlenen dosyaları BigQuery'den al
    query = f"SELECT file_name FROM `{control_table_ref}`"
    processed_files = set(row.file_name for row in bq_client.query(query).result())

    # Bucket içindeki tüm dosyaları listele
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=FOLDER_PATH)

    new_files = []
    for blob in blobs:
        file_name = blob.name
        if file_name.endswith('.parquet') and file_name not in processed_files:
            print(f"Processing new file: {file_name}")

            # Dosyayı indir ve oku
            parquet_bytes = blob.download_as_bytes()
            parquet_file = io.BytesIO(parquet_bytes)
            table = pq.read_table(parquet_file)
            df = table.to_pandas()

            # BigQuery'ye yükle
            job = bq_client.load_table_from_dataframe(df, raw_table_ref)
            job.result()  # İş bitene kadar bekle

            # Kontrol tablosuna ekle
            rows_to_insert = [{
                'file_name': file_name,
                'processed_at': datetime.utcnow().isoformat()
            }]
            bq_client.insert_rows_json(control_table_ref, rows_to_insert)

            print(f"Finished processing: {file_name}")
            new_files.append(file_name)

    return f"Processed {len(new_files)} new files."

