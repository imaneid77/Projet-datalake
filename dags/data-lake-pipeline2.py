from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
#from airflow.providers.http.operators.http import SimpleHttpOperator
try:
    from airflow.providers.http.operators.http import SimpleHttpOperator
except ImportError:
    print("Le module SimpleHttpOperator n'est pas disponible. Vérifiez votre installation.")

from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchTaskHandler
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from src.preprocess_to_staging import main as preprocess_main
from src.process_to_curated2 import main as process_main
from build.unpack_to_raw_v import main as unpack_main

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 12),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry':False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_lake_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour le traitement des données',
    schedule=timedelta(days=1),
    catchup=False,
)

check_elasticsearch = SimpleHttpOperator(
    task_id="check_elasticsearch",
    http_conn_id="elasticsearch_default",
    endpoint="_cluster/health",
    method="GET",
    dag=dag,
)

# Il faut attendre la présence des données dans RAW
wait_for_raw_data = FileSensor(
    task_id='wait_for_raw_data',
    filepath='/opt/airflow/data/raw/bigtech_combined.csv',  # 📌 Modifier si nécessaire
    poke_interval=60,
    timeout=600, 
    mode='poke',
    dag=dag,
)

#raw
extract_task = PythonOperator(
    task_id='unpack_to_raw',
    python_callable=unpack_main,
    op_kwargs={'output_dir': '/opt/airflow/data/raw', 'endpoint_url': 'http://localstack:4566'},
    dag=dag,
)

#staging
transform_task = PythonOperator(
    task_id='preprocess_to_staging',
    python_callable=preprocess_main,
    op_kwargs={
        'bucket_raw': 'raw',
        'db_host': 'mysql',
        'db_user': 'root',
        'db_password': 'root',
        'endpoint_url': 'http://localstack:4566',
    },
    dag=dag,
)

#curated
load_task = PythonOperator(
    task_id='process_to_curated',
    python_callable=process_main,
    op_kwargs={
        'mysql_host': 'mysql',
        'mysql_user': 'root',
        'mysql_password': 'root',
        'mongo_uri': 'mongodb://localhost:27017/',
        'mongo_db': 'bigtech_db',
        'mongo_collection': 'tweets',
    },
    trigger_rule=TriggerRule.ALL_DONE,  #on lance meme si une tâche précédente échoue
    dag=dag,
)

def log_failure_to_elasticsearch(context):
    """Stocke les logs des tâches échouées dans Elasticsearch."""
    log_msg = {
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        "execution_date": str(context["execution_date"]),
        "log_url": context["task_instance"].log_url,
        "exception": str(context.get("exception", "N/A")),
    }

    es_conn = BaseHook.get_connection("elasticsearch_default")
    es_host = es_conn.host

    try:
        es_handler = ElasticsearchTaskHandler(
            base_log_folder="/opt/airflow/logs",
            log_id_template="{dag_id}-{task_id}-{execution_date}-{try_number}",
            json_format=True,
            end_of_log_mark="end_of_log"
        )
        es_handler.log.info(log_msg)
        print(f"Log enregistré dans Elasticsearch: {log_msg}")
    except Exception as e:
        print(f"Erreur lors de l'enregistrement dans Elasticsearch: {e}")

# Ajout de l’alerte Elasticsearch en cas d’échec
default_args["on_failure_callback"] = log_failure_to_elasticsearch


#ordre des tâches
check_elasticsearch >> wait_for_raw_data
wait_for_raw_data >> extract_task
extract_task >> transform_task
transform_task >> load_task
