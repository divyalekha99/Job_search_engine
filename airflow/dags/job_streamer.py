from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from docker.types import Mount
import os

def one_day_ago():
    return datetime.utcnow() - timedelta(days=1)

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROJECT_ROOT = '/Users/divyalekhas/Pet_Projects/Automated_Job_Finder/'

with DAG(
    dag_id='job_streamer',
    default_args=default_args,
    description='Scrape → Kafka → Consume → Spark',
    schedule='@hourly',
    start_date=one_day_ago(),
    catchup=False,
) as dag:

    scrape = DockerOperator(
        task_id='scrape_jobs',
        image='automated_job_finder-scraper:latest',
        command='python job_scraper.py',
        network_mode='automated_job_finder_default',
        mounts=[Mount(source=os.path.join(PROJECT_ROOT, 'data/state'), target='/data/state', type='bind')],
        docker_url='unix://var/run/docker.sock',
        auto_remove='force',
    )

    consume = DockerOperator(
        task_id='consume_jobs',
        image='automated_job_finder-consumer:latest',
        command='python consumer.py',
        network_mode='automated_job_finder_default',
        mounts=[Mount(source=os.path.join(PROJECT_ROOT, 'data/raw'), target='/data/raw', type='bind')],
        docker_url='unix://var/run/docker.sock',
        auto_remove='force',
    )

    spark_ingest = SparkSubmitOperator(
        task_id='spark_ingest',
        application='/app/spark_ingest.py',
        conn_id='spark_default',
        application_args=[
            '--master', 'local[*]',
            '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0',
            '--bootstrap-servers', 'kafka:9092',
            '--topic', 'jobs',
            '--output-path', '/data/bronze'
        ],
    )

    scrape >> consume >> spark_ingest