from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='A simple data pipeline example',
    schedule_interval=timedelta(seconds=60),
)

task_kafka_producer = BashOperator(
    task_id='run_kafka_producer',
    bash_command='python /opt/airflow/kafka/kafka_producer.py',
    dag=dag,
)

task_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --deploy-mode client /opt/airflow/spark/spark_job.py',
    dag=dag,
)

task_kafka_producer >> task_spark_job
