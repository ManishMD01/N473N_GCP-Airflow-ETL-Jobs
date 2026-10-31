#
# DAG: JSON to BigQuery Data Load Pipeline
# Description: Loads JSON data from GCS into BigQuery (raw_customers, raw_transactions).
#

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

GCP_PROJECT_ID = 'nalen-458109'
GCS_BUCKET = 'gemini-de-bucket'
BQ_DATASET = 'fincrime_er'

CUSTOMERS_SCHEMA = [
    {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'dob', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'ssn', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'zip', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'device_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'ip_address', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'source_system', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'load_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'load_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'batch_id', 'type': 'STRING', 'mode': 'NULLABLE'},
]

TRANSACTIONS_SCHEMA = [
    {'name': 'transaction_id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'sender_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'receiver_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'amount', 'type': 'NUMERIC', 'mode': 'REQUIRED'},
    {'name': 'currency', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'transaction_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transaction_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'beneficiary_account', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'source_system', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'load_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'load_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'batch_id', 'type': 'STRING', 'mode': 'NULLABLE'},
]

args = {'owner': 'Manish-Dhodare', 'gcp_conn_id': 'google_cloud_default'}

with DAG(
    dag_id='etl_json_to_bigquery_load',
    default_args=args,
    schedule='0 5 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['json', 'bigquery', 'etl'],
) as dag:
    load_customers = GCSToBigQueryOperator(
        task_id='load_customers_json_to_bq',
        bucket=GCS_BUCKET,
        source_objects=['customers.json'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.raw_customers',
        schema_fields=CUSTOMERS_SCHEMA,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
    )

    load_transactions = GCSToBigQueryOperator(
        task_id='load_transactions_json_to_bq',
        bucket=GCS_BUCKET,
        source_objects=['transactions.json'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.raw_transactions',
        schema_fields=TRANSACTIONS_SCHEMA,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
    )

    quality_check = BigQueryInsertJobOperator(
        task_id='bq_data_quality_check',
        configuration={
            'query': {
                'query': f"SELECT count(*) as count FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_customers`",
                'useLegacySql': False,
            }
        },
        gcp_conn_id=args['gcp_conn_id'],
    )

    [load_customers, load_transactions] >> quality_check
