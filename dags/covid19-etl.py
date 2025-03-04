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
    # month, day = result[0], result[1]
    
    # if month == None: month = 1
    # if day == None: day = 1

    month, day = 3, 1
    data = pd.DataFrame()
    while month <= current_date.month:
        
        for year in ['2020', '2021', '2022', '2023']:

            try:
                date = f'{str(month).zfill(2)}-{str(day).zfill(2)}-{year}'
                url = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{date}.csv'
                df = pd.read_csv(url)
                logging.info(f'Extracted data for {date}')
                df.columns = df.columns.str.replace('/', '_').str.replace(' ', '_')
                df.columns = [col.replace('Long_', 'Long') if 'Long' in col else col for col in df.columns]
                df.columns = [col.replace('Longitude', 'Long') if 'Long' in col else col for col in df.columns]
                df.columns = [col.replace('Latitude', 'Lat') if 'Lat' in col else col for col in df.columns]
                columns_to_keep = ['Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long', 'Confirmed', 'Deaths', 'Recovered']
                df = df[columns_to_keep]
                data = pd.concat([data, df])

            except Exception as e:
                logging.error(f'Error extracting data for {date}: {e}')

        day += 1
        if month == current_date.month and day > current_date.day:
            break
        if day > 31:
            month += 1
            day = 1

    logging.info(f'Data Columns: {data.columns}')
    logging.info(f'Data Shape: {data.shape}')
    ti.xcom_push(key='data', value=data.to_dict())


def transform(ti):
    logging.info('Transforming data')

    
    data = ti.xcom_pull(key='data', task_ids='extract')
    data = pd.DataFrame.from_dict(data)
    data['Last_Update'] = pd.to_datetime(data['Last_Update']).dt.strftime('%Y-%m-%d')
    data['Province_State'] = data['Province_State'].fillna('Unknown')
    data['Confirmed'] = data['Confirmed'].fillna(0)
    data['Deaths'] = data['Deaths'].fillna(0)
    data['Recovered'] = data['Recovered'].fillna(0)
    data['Country_Region'] = data['Country_Region'].replace('US', 'United States')
    data['Confirmed'] = data['Confirmed'].astype(int)
    data['Deaths'] = data['Deaths'].astype(int)
    data['Recovered'] = data['Recovered'].astype(int)

    
    logging.info(f'Data Description:\n{data.describe()}')
    logging.info(f'Data Info:\n{data.info()}')

    ti.xcom_push(key='data', value=data.to_dict())


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
    'catchup': False
}
with DAG('covid19_etl', default_args=default_args, schedule_interval='@daily') as dag:
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract
    )
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    extract >> transform