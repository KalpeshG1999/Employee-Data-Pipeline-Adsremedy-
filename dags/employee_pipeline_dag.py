from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'kalpesh',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'employee_data_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
)

run_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command="""
    docker exec spark_container spark-submit \
    --jars /home/jovyan/work/jars/postgresql-42.7.3.jar \
    /home/jovyan/work/spark/app.py
    """,
    dag=dag
)

run_spark_job