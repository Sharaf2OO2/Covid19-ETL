import logging, pandas as pd ,math
from typing import Tuple
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch

def get_last_processed_date() -> Tuple[int, int]:
    """Get the last processed date from dim_date table."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT MAX(month), MAX(day) FROM dim_date')
            month, day = cursor.fetchone()
            return month or 1, day or 0

def get_valid_date(month: int, day: int) -> Tuple[int, int]:
    """Get next valid date given month and day."""
    day += 1
    while True:
        try:
            datetime.strptime(f"2020-{str(month).zfill(2)}-{str(day).zfill(2)}", "%Y-%m-%d")
            return month, day
        except ValueError:
            day += 1
            if day > 31:
                month += 1
                day = 1

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names in DataFrame."""
    df.columns = df.columns.str.replace('/', '_').str.replace(' ', '_')
    df.columns = [col.replace('Long_', 'Long') if 'Long' in col else col for col in df.columns]
    df.columns = [col.replace('Longitude', 'Long') if 'Long' in col else col for col in df.columns]
    df.columns = [col.replace('Latitude', 'Lat') if 'Lat' in col else col for col in df.columns]
    return df

def extract(ti) -> None:
    """Extract COVID-19 data from source."""
    month, day = get_last_processed_date()
    month, day = get_valid_date(month, day)
    
    data = pd.DataFrame()
    logging.info('Extracting data from source')
    
    for year in ['2020', '2021', '2022', '2023']:
        try:
            date = f'{str(month).zfill(2)}-{str(day).zfill(2)}-{year}'
            url = f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{date}.csv'
            df = pd.read_csv(url)
            logging.info(f'Extracted data for {date}')
            
            df = clean_column_names(df)
            columns_to_keep = ['Province_State', 'Country_Region', 'Last_Update', 
                             'Lat', 'Long', 'Confirmed', 'Deaths', 'Recovered']
            df = df[columns_to_keep]
            df['Last_Update'] = date
            data = pd.concat([data, df])

        except Exception as e:
            logging.error(f'Error extracting data for {date}: {e}')

    ti.xcom_push(key='data', value=data.to_json(orient='records'))

def transform(ti):
    logging.info('Transforming data')

    data = ti.xcom_pull(key='data', task_ids='Extract')
    data = pd.read_json(data)
    data['Last_Update'] = pd.to_datetime(data['Last_Update']).dt.strftime('%Y-%m-%d')
    data['Province_State'] = data['Province_State'].fillna('Unknown')
    data['Confirmed'] = data['Confirmed'].fillna(0)
    data['Deaths'] = data['Deaths'].fillna(0)
    data['Recovered'] = data['Recovered'].fillna(0)
    data['Country_Region'] = data['Country_Region'].replace('US', 'United States')

    logging.info(f'Data Description:\n{data.describe()}')
    logging.info(f'Data Info:\n{data.info()}')
    ti.xcom_push(key='data', value=data.to_json())

def prepare_dim_tables(data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare dimension tables for loading."""
    dim_date = data[['year', 'month', 'month_name', 'day', 'quarter', 'date_id']]
    
    dim_location = data[['Country_Region', 'Province_State', 'Lat', 'Long']]
    dim_location['location_id'] = dim_location.apply(
        lambda row: f"{row['Country_Region'].replace(' ', '-')}_{row['Province_State'].replace(' ', '-')}_{row['Lat']}_{row['Long']}", 
        axis=1
    )
    
    return dim_date, dim_location

def load(ti):
    logging.info('Loading data to destination')

    data = ti.xcom_pull(key='data', task_ids='Transform')
    data_full = pd.read_json(data)
    data_full['Last_Update'] = pd.to_datetime(data_full['Last_Update'])
    data_full['Confirmed'] = data_full['Confirmed'].astype(float)
    data_full['Deaths'] = data_full['Deaths'].astype(float)
    data_full['Recovered'] = data_full['Recovered'].astype(float)
    data_full['year'] = data_full['Last_Update'].dt.year.astype(float)
    data_full['month'] = data_full['Last_Update'].dt.month.astype(float)
    data_full['month_name'] = data_full['Last_Update'].dt.month_name()
    data_full['day'] = data_full['Last_Update'].dt.day.astype(float)
    data_full['quarter'] = data_full['Last_Update'].dt.quarter.astype(float)
    data_full['date_id'] = data_full['Last_Update'].dt.strftime('%Y%m%d').astype(float)
    data_full['location_id'] = data_full.apply(lambda row: f"{row['Country_Region'].replace(' ', '-')}_{row['Province_State'].replace(' ', '-')}_{row['Lat']}_{row['Long']}", axis=1)

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    
    # Calculate total batches needed
    batch_size = 200
    total_rows = len(data_full)
    total_batches = math.ceil(total_rows / batch_size)
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for batch in range(total_batches):
                start_idx = batch * batch_size
                end_idx = min((batch + 1) * batch_size, total_rows)
                
                data = data_full.iloc[start_idx:end_idx]
                
                # Process dimension tables
                dim_date, dim_location = prepare_dim_tables(data)
                execute_batch(cursor, '''
                    INSERT INTO dim_date (year, month, month_name, day, quarter, date_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date_id) DO NOTHING
                ''', dim_date.to_records(index=False))

                execute_batch(cursor, '''
                    INSERT INTO dim_location (region, state, lat, long, location_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (location_id) DO NOTHING
                ''', dim_location.to_records(index=False))

                fact_covid_cases = data[['date_id', 'location_id', 'Confirmed', 'Deaths', 'Recovered']]
                execute_batch(cursor, '''
                    INSERT INTO fact_covid_cases (date_id, location_id, cases, deaths, recoveries)
                    VALUES (%s, %s, %s, %s, %s)
                ''', fact_covid_cases.to_records(index=False))

                logging.info(f'Processed batch {batch + 1}/{total_batches} ({end_idx}/{total_rows} rows)')
            
            conn.commit()
            logging.info(f'Successfully loaded all {total_rows} rows to destination')

default_args = {
    'owner': 'Sharaf',
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'covid19_etl',
    default_args=default_args,
    schedule_interval="*/3 * * * *",
    catchup=False,
    doc_md="""
    # COVID-19 ETL Pipeline
    This DAG extracts daily COVID-19 data, transforms it, and loads it into a data warehouse.
    
    ## Tasks
    1. Create dimension tables
    2. Extract data from source
    3. Transform data
    4. Load data into warehouse
    """
) as dag:
    extract = PythonOperator(
        task_id='Extract',
        python_callable=extract
    )
    transform = PythonOperator(
        task_id='Transform',
        python_callable=transform
    )
    load = PythonOperator(
        task_id='Load',
        python_callable=load
    )
    create_dim_date = PostgresOperator(
        task_id='create_dim_date',
        postgres_conn_id='postgres_localhost',
        sql= """
            create table if not exists dim_date
            (
                year       integer,
                month      integer,
                month_name varchar,
                day        integer,
                quarter    integer,
                date_id    integer not null primary key
            );
        """
    )
    create_dim_location = PostgresOperator(
        task_id='create_dim_location',
        postgres_conn_id='postgres_localhost',
        sql= """
         create table if not exists dim_location
        (
            region       varchar,
            lat          double precision,
            long         double precision,
            state        varchar,
            location_id  varchar primary key
        );
        """
    )
    create_fact_covid_cases = PostgresOperator(
        task_id='create_fact_covid_cases',
        postgres_conn_id='postgres_localhost',
        sql= """
        create table if not exists fact_covid_cases
        (
            date_id     integer constraint date_fk references dim_date,
            location_id varchar constraint location_fk references dim_location,
            cases       int,
            deaths      int,
            recoveries  int,
            case_id     serial primary key
        );
        """
    )

[create_dim_date, create_dim_location] >> create_fact_covid_cases >> extract >> transform >> load