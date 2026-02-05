
import sys
sys.path.insert(0, "/opt/airflow/project")


from datetime import datetime,timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator




def run_store_data():
    # import inside task so DAG parsing stays fast
    from youtube_ingestion_pipeline import store_data
    return store_data()

def run_chunk_and_index():
    from rag_indexing_pipeline import chunk_and_index
    return chunk_and_index()

with DAG(
    dag_id="Tech_Expert",
    description="keep ingesting new data into our database",
    schedule="0 9 * * 1",
    start_date=datetime(2026, 1, 20),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    t1 = PythonOperator(
        task_id="store_in_database",
        python_callable=run_store_data,
    )

    t2 = PythonOperator(
        task_id="chunk_and_index",
        python_callable=run_chunk_and_index,
    )

    t1 >> t2
