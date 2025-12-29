from datetime import timedelta

from airflow.models import Variable
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.sdk import DAG

BQ_PROJECT = Variable.get("BQ_PROJECT")
BQ_DATASET = Variable.get("BQ_DATASET")
BQ_TABLE_ID = "users"
GC_BUCKET = Variable.get("GC_BUCKET")

with DAG(
    "bigquery_to_gcs",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="BigQuery to GCS DAG",
    tags=["test"],
) as dag:

    bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs_export",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_ID}",
        destination_cloud_storage_uris=[f"gs://{GC_BUCKET}/users.jsonl.gz"],
        export_format="NEWLINE_DELIMITED_JSON",
        compression="GZIP",
    )

    bigquery_to_gcs