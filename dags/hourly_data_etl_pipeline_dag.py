import os
import requests
import csv
import logging
from datetime import datetime, timedelta
from io import StringIO
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


api_key = os.environ.get('OPENAQ_API_KEY')
# s3_bucket_name = os.environ.get("S3_BUCKET")
s3_bucket_name = "air-quality-bucket-ulaanbaatar"





default_args = {
    'owner': 'sumiya',
    'start_date': datetime(2023, 11, 27),
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}


def fetch_openaq_data_and_upload(**kwargs):
    
    end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

    # Create a logic to pull historic data if it's DAG start date
    execution_date = kwargs['ds']

    if execution_date == kwargs['dag'].default_args['start_date'].strftime("%Y-%m-%d"):
        # Define start date
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S%z")
    else:
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S%z")

    # OpenAQ API URL 
    api_url = f"https://api.openaq.org/v2/measurements?location_id=72184&parameter=temperature&parameter=pm25&date_from={start_date}&date_to={end_date}&limit=1700"

    # Pull data from OpenAQ API
    response = requests.get(api_url, headers={"X-API-Key": api_key})

    header = ["id", "name", "hour", "day", "month", "year", "hod", "dow", "average",
          "measurement_count", "parameter", "parameterId", "displayName", "unit",
          "first_datetime", "last_datetime"]

    if response.status_code == 200:
        data_to_upload = response.json()['results']
    
        records_to_insert = []
        for record in data_to_upload:
            records_to_insert.append((
                record['locationId'],
                record['location'],
                record['parameter'],
                record['value'],
                record['date']['utc'],
                record['date']['local'],
                record['unit'],
                record['coordinates']['latitude'],
                record['coordinates']['longitude'],
                record['country'],
                record['city'],
                record['isMobile'],
                record['isAnalysis'],
                record['entity'],
                record['sensorType']
            ))
        
        s3_hook = S3Hook(aws_conn_id = "aws_connection")
        s3_key = f"pm25-hourly-{execution_date}.csv"
   

        with open("csv_to_upload_to_s3", 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(header)
            csv_writer.writerows(records_to_insert)

        s3_hook.load_file(
            filename=csv_file.name,
            key=s3_key,
            bucket_name=s3_bucket_name,
            replace=True
            )
    else:
        logging.info("Failed to fetch data from OpenAQ API")


def retrieve_csv_from_s3(**kwargs):

    execution_date = kwargs['ds']

    s3_hook = S3Hook(aws_conn_id = "aws_connection")
    s3_key = f"pm25-hourly-{execution_date}.csv"

    s3_object = s3_hook.get_key(key=s3_key, bucket_name=s3_bucket_name)
    if s3_object is not None:
        csv_data = s3_object.get()['Body'].read().decode('utf-8')
        return csv_data
    else:
        raise FileNotFoundError(f"File {s3_key} not found in bucket {s3_bucket_name}")


def upload_to_postgres(**kwargs):

    schema_name = 'public'
    table_name = 'openaq_hourly_data' 
        
    csv_data = retrieve_csv_from_s3(**kwargs)
    postgres_hook = PostgresHook(postgres_conn_id="postgres_localhost")
    postgres_connection = postgres_hook.get_conn()
    # cursor = postgres_connection.cursor()

    copy_query = f"COPY {schema_name}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ','"
    with postgres_connection.cursor() as cur:
        cur.copy_expert(copy_query, StringIO(csv_data))

    postgres_connection.commit()
    postgres_connection.close()


dag = DAG('dag_for_hourly_data_1',
        default_args=default_args, 
        schedule_interval="@daily"
        )

fetch_openaq_data_and_upload_task = PythonOperator(
    task_id = 'fetch_openaq_data_and_upload_task',
    python_callable = fetch_openaq_data_and_upload,
    dag = dag
)


create_postgres_table_task = PostgresOperator(
    task_id='create_postgres_table_task',
    postgres_conn_id='postgres_localhost',
    sql='''
        CREATE TABLE IF NOT EXISTS openaq_hourly_data (
            location_id INTEGER,
            location VARCHAR(100),
            parameter VARCHAR(50),
            value NUMERIC,
            utc_date TIMESTAMP,
            local_date TIMESTAMP,
            unit VARCHAR(20),
            latitude NUMERIC,
            longitude NUMERIC,
            country VARCHAR(2),
            city VARCHAR(100),
            is_mobile BOOLEAN,
            is_analysis BOOLEAN,
            entity VARCHAR(100),
            sensor_type VARCHAR(50)
        );
    ''',
    dag = dag
)

upload_csv_to_postgres_task = PythonOperator(
    task_id = "upload_csv_to_postgres_task",
    python_callable = upload_to_postgres,
    dag = dag
)

create_processed_data_table_task = PostgresOperator(
    task_id='create_processed_data_table_task',
    sql='''
    CREATE TABLE IF NOT EXISTS pm25_hourly_data(
        local_date TIMESTAMP,
        hour INTEGER,
        pm25 NUMERIC(5, 1),
        temperature NUMERIC(10, 2),
        CONSTRAINT unique_local_date_constraint UNIQUE (local_date)
    );
    ''',
    postgres_conn_id='postgres_localhost',
    dag=dag
)

insert_processed_data_task = PostgresOperator(
    task_id = 'insert_processed_data_task',
    sql='''
    INSERT INTO pm25_hourly_data (local_date, hour, pm25, temperature)
    (SELECT local_date,
        MAX(EXTRACT(HOUR FROM local_date)),
        MAX(CASE WHEN parameter = 'pm25' THEN value END) AS pm25,
        (MAX(CASE WHEN parameter = 'temperature' THEN (((value - 32) * 5) / 9) END)) as temperature
    FROM openaq_hourly_data
    GROUP BY local_date)
    ON CONFLICT ON CONSTRAINT unique_local_date_constraint DO NOTHING;
    ''',
    postgres_conn_id='postgres_localhost',
    dag=dag
)

fetch_openaq_data_and_upload_task >> create_postgres_table_task >> upload_csv_to_postgres_task >> create_processed_data_table_task >> insert_processed_data_task