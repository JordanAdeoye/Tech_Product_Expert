
import sys
sys.path.insert(0, "/opt/airflow/project")

# from youtube_ingestion_pipeline import store_data
# from rag_indexing_pipeline import chunk_and_index

from datetime import datetime,timedelta

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator

# # A Dag represents a workflow, a collection of tasks
# with DAG(
#     "Tech_Expert",
#     # These args will get passed on to each operator
#     # You can override them on a per-task basis during operator initialization
#     default_args={
#         "depends_on_past": False,
#         "retries": 1,
#         "retry_delay": timedelta(minutes=5),
#     },
#     description="keep ingesting new data into our database",
#     schedule="*/20 * * * *",
#     start_date=datetime(2025, 12, 25),
#     catchup=False,
#     tags=["tech_talk"],
# ) as dag:

#     # t1, t2 and t3 are examples of tasks created by instantiating operators
#     t1 = PythonOperator(
#         task_id="StoreinDatabse",
#         python_callable=store_data,

#     )

#     t2 = PythonOperator(
#         task_id="Chunk&IndexinChromadb",
#         python_callable=chunk_and_index,
#     )

#     t1 >> t2


from datetime import datetime, timedelta


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
    schedule="*/20 * * * *",
    start_date=datetime(2025, 12, 25),
    catchup=False,
    default_args={
        "retries": 1,
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
