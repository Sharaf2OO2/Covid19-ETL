import logging, pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta

def extract(ti):
    logging.info('Extracting data from source')

    current_date = datetime.now()
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('''
                    SELECT MAX(month), MAX(day)
                    FROM dim_date
        ''')
    result = cursor.fetchone()
    month, day = result[0], result[1]
    
    if month == None: month = 1
    if day == None: day = 1

    while month <= current_date.month:
        
        for year in ['2020', '2021', '2022', '2023']:

            try:
                date = f'{str(month).zfill(2)}-{str(day).zfill(2)}-{year}'
                url = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{date}.csv'
                df = pd.read_csv(url)
                logging.info(f'Extracted data for {date}')
                ti.xcom_push(key=f'covid19-data-{date}', value=df.to_json())     

            except Exception as e:
                logging.error(f'Error extracting data for {date}: {e}')

        day += 1
        if month == current_date.month and day > current_date.day:
            break
        if day > 31:
            month += 1
            day = 1



def transform():
    pass


def load():
    pass


default_args = {
    'owner': 'Sharaf',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
with DAG('covid19_etl', default_args=default_args, schedule_interval='@daily') as dag:
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    extract