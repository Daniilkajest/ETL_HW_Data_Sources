import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

DATASET_PATH = '/opt/airflow/dags/IOT-temp.csv'
TABLE_NAME = 'weather_telemetry'

def transform_data():
    df = pd.read_csv(DATASET_PATH)
    df['noted_date'] = pd.to_datetime(df['noted_date'], dayfirst=True, errors='coerce').dt.date
    df['out/in'] = df['out/in'].str.lower().str.strip()
    df = df[df['out/in'] == 'in']
    # Очистка по процентилям
    lower_limit = df['temp'].quantile(0.05)
    upper_limit = df['temp'].quantile(0.95)
    df_cleaned = df[(df['temp'] >= lower_limit) & (df['temp'] <= upper_limit)].copy()
    return df_cleaned

def full_load():
    df = transform_data()
    pg_hook = PostgresHook(postgres_conn_id='postgres_default') # Твое соединение
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Загружаем всё целиком 
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print(f"Full load completed. Total rows: {len(df)}")

def incremental_load():
    df = transform_data()
    
    max_date = df['noted_date'].max()
    cutoff_date = max_date - timedelta(days=3)
    
    df_incremental = df[df['noted_date'] >= cutoff_date]
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    df_incremental.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    print(f"Incremental load completed. Added rows: {len(df_incremental)}")

default_args = {
    'owner': 'Сидоров Даниил',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='sidorov_d_d_dz4_loading',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dz4', 'loading']
) as dag:

    task_full_load = PythonOperator(
        task_id='full_historical_load',
        python_callable=full_load
    )

    task_incremental_load = PythonOperator(
        task_id='incremental_delta_load',
        python_callable=incremental_load
    )

    task_full_load >> task_incremental_load