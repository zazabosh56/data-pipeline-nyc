from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_weather_table():
    conn = psycopg2.connect(
        dbname='airflow',
        user='airflow',
        password='airflow',
        host='postgres',
        port=5432
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather_data (
                    timestamp   TIMESTAMP,
                    temperature FLOAT,
                    humidity    FLOAT,
                    pressure    FLOAT,
                    city        VARCHAR(50)
                );
            """)
    conn.close()

def create_dim_weather_table():
    conn = psycopg2.connect(
        dbname='airflow',
        user='airflow',
        password='airflow',
        host='postgres',
        port=5432
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_weather (
                    timestamp         TIMESTAMP,
                    temperature       FLOAT,
                    humidity          FLOAT,
                    wind_speed        FLOAT,
                    weather_condition VARCHAR(50),
                    weather_category  VARCHAR(20),
                    hour              INT,
                    day_of_week       INT,
                    city              VARCHAR(50)
                );
            """)
    conn.close()

def run_weather_download():
    subprocess.run(["python", "scripts/load_weather.py"], check=True)

def run_weather_streaming_processing():
    subprocess.run([
        "spark-submit",
        "--jars", "/opt/airflow/jars/postgresql-42.2.18.jar",
        "/opt/airflow/spark_jobs/stream_weather_processor.py"
    ], check=True)

with DAG(
    dag_id='weather_streaming_pipeline',
    default_args=default_args,
    description='Pipeline météo : collecte, streaming et transformation',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'streaming'],
) as dag:

    create_weather_table_task = PythonOperator(
        task_id='create_weather_table',
        python_callable=create_weather_table
    )

    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=run_weather_download
    )

    stream_task = PythonOperator(
        task_id='process_streaming_weather_data',
        python_callable=run_weather_streaming_processing
    )

    create_dim_weather_task = PythonOperator(
        task_id='create_dim_weather_table',
        python_callable=create_dim_weather_table
    )

    transform_dim_weather = BashOperator(
        task_id='transform_dim_weather',
        bash_command='spark-submit --jars /opt/airflow/jars/postgresql-42.2.18.jar /opt/airflow/spark_jobs/transform_dim_weather.py'
    )

    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
)

    # Chaînage avec dbt_run en fin
    create_weather_table_task >> fetch_task >> stream_task >> create_dim_weather_task >> transform_dim_weather >> dbt_run
