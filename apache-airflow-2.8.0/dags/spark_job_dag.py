from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    'owner': 'limoo',
    'start_date': datetime(2023,12,11),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='spark_dag',
    default_args=default_args,
    description='Orchestration of data generation, processing and persistence into a data warehouse',
    schedule='*/3 * * * *',
)
run_spark_task = SparkSubmitOperator(
    task_id='spark_script',
    application='./dags/spark_job.py',
    conn_id="spark_default",
    verbose=False,
    dag=dag,
)

run_spark_task
