from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_lake_pipeline_raw_staging',
    default_args=default_args,
    description='Pipeline ETL pour le traitement des données : Raw et Staging',
    schedule_interval=timedelta(days=1),
)

# Tâche 1: Extraction des données brutes et stockage dans le bucket S3 "raw"
unpack_to_raw = BashOperator(
    task_id='unpack_to_raw',
    bash_command='python /opt/airflow/build/unpack_to_raw.py --output-dir /opt/airflow/data/raw --endpoint-url http://localstack:4566',
    dag=dag,
)

# Tâche 2: Transformation et chargement des données dans MySQL (couche Staging)
preprocess_to_staging = BashOperator(
    task_id='preprocess_to_staging',
    bash_command='python /opt/airflow/scripts/preprocess_to_staging.py --bucket_raw raw --db_host mysql --db_user root --db_password root --endpoint-url http://localstack:4566',
    dag=dag,
)

# Ordre d'exécution : unpack_to_raw doit être exécuté avant preprocess_to_staging
unpack_to_raw >> preprocess_to_staging
