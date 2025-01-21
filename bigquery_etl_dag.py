from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
with DAG(
    'bigquery_etl',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:
    
    query_task = BigQueryExecuteQueryOperator(
    task_id='run_bigquery_etl',
    sql="""
        WITH customer_analysis AS (
            SELECT 
                *,
                CASE 
                    WHEN tenure >= 24 THEN 'Long Term'
                    WHEN tenure >= 12 THEN 'Medium Term'
                    ELSE 'New Customer'
                END as customer_segment,
                CURRENT_TIMESTAMP() as etl_timestamp
            FROM `etl-pipeline-444417.etl_dataset.main_table`
        )
        SELECT * FROM customer_analysis
    """,
    use_legacy_sql=False,
    destination_dataset_table='etl-pipeline-444417.etl_dataset.processed_customer_data',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)