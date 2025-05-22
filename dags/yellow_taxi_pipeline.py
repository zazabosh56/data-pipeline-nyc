from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_taxi_table():
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
                CREATE TABLE IF NOT EXISTS taxi_data (
                    VendorID INT,
                    tpep_pickup_datetime TIMESTAMP,
                    tpep_dropoff_datetime TIMESTAMP,
                    passenger_count INT,
                    trip_distance FLOAT,
                    fare_amount FLOAT,
                    tip_amount FLOAT,
                    total_amount FLOAT,
                    pickup_date DATE,
                    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime)
                );
            """)
    conn.close()

def create_fact_taxi_trips_table():
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
                CREATE TABLE IF NOT EXISTS fact_taxi_trips (
                    VendorID INT,
                    pickup_time TIMESTAMP,
                    dropoff_time TIMESTAMP,
                    trip_duration FLOAT,
                    distance_bin VARCHAR(10),
                    payment_type VARCHAR(20),
                    tip_percent FLOAT,
                    pickup_hour INT,
                    pickup_day_of_week INT,
                    pickup_zone VARCHAR(50),
                    dropoff_zone VARCHAR(50),
                    PRIMARY KEY (VendorID, pickup_time, dropoff_time)
                );
            """)
    conn.close()

with DAG(
    dag_id='yellow_taxi_pipeline',
    default_args=default_args,
    description='Pipeline ingestion + transformation Yellow Taxi',
    schedule_interval='@monthly',
    start_date=days_ago(1),
    catchup=False,
    tags=['nyc', 'taxi'],
) as dag:

    create_taxi_table_task = PythonOperator(
        task_id='create_taxi_table',
        python_callable=create_taxi_table
    )
    load_task = BashOperator(
        task_id='load_yellow_taxi_data',
        bash_command="python /opt/airflow/scripts/load_yellow_taxi.py"
    )

    transform_task = BashOperator(
        task_id='pyspark_transformation',
        bash_command="spark-submit /opt/airflow/spark_jobs/transform_yellow_taxi.py "
                    "--input_path /opt/airflow/data/raw/taxi/yellow_tripdata_2023-01.parquet "
                    "--output_path /opt/airflow/data/processed/yellow_taxi"
    )

    store_task = BashOperator(
        task_id='store_to_dwh',
        bash_command="python /opt/airflow/scripts/store_to_dwh.py"
    )

    create_fact_taxi_trips_task = PythonOperator(
        task_id='create_fact_taxi_trips_table',
        python_callable=create_fact_taxi_trips_table
    )

    transform_fact_taxi_trips = BashOperator(
        task_id='transform_fact_taxi_trips',
        bash_command='spark-submit --jars /opt/airflow/jars/postgresql-42.2.18.jar /opt/airflow/spark_jobs/transform_fact_taxi_trips.py'
    )

    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project',
)

    # Chaînage complet avec dbt_run en fin
    create_taxi_table_task >> load_task >> transform_task >> store_task >> create_fact_taxi_trips_task >> transform_fact_taxi_trips >> dbt_run
