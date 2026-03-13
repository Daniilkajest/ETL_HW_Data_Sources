import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


DATASET_PATH = '/opt/airflow/dags/IOT-temp.csv' 

def transform_weather_data():
    # 1. Загрузка
    df = pd.read_csv(DATASET_PATH)
    
    
    df['noted_date'] = pd.to_datetime(df['noted_date'], dayfirst=True, errors='coerce').dt.date
    
    # 3. Фильтруем out/in
    df['out/in'] = df['out/in'].str.lower().str.strip()
    df = df[df['out/in'] == 'in']
    
    # Проверка: если после фильтрации пусто, попробуем взять 'out' или все данные
    if df.empty:
        print("Внимание: фильтр 'in' не нашел данных. Используем весь датасет для расчета.")
        df = pd.read_csv(DATASET_PATH)
        df['noted_date'] = pd.to_datetime(df['noted_date']).dt.date
    
    # 4. Очищаем температуру по 5-му и 95-му процентилю
    lower_limit = df['temp'].quantile(0.05)
    upper_limit = df['temp'].quantile(0.95)
    df_cleaned = df[(df['temp'] >= lower_limit) & (df['temp'] <= upper_limit)].copy()
    
    # 5. Вычисляем 5 самых жарких и самых холодных дней
    # Группируем по дате, чтобы найти среднюю температуру за день
    daily_temp = df_cleaned.groupby('noted_date')['temp'].mean().reset_index()
    
    top_5_hot = daily_temp.nlargest(5, 'temp')
    top_5_cold = daily_temp.nsmallest(5, 'temp')
    
    print("--- 5 самых жарких дней (средняя темп.) ---")
    print(top_5_hot)
    print("\n--- 5 самых холодных дней (средняя темп.) ---")
    print(top_5_cold)
    
    # Сохраняем результат для проверки
    output_path = '/opt/airflow/dags/transformed_data_dz3.csv'
    df_cleaned.to_csv(output_path, index=False)
    print(f"Файл успешно сохранен: {output_path}")

default_args = {
    'owner': 'Сидоров Даниил',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='sidorov_d_d_dz3_transformation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dz3', 'transformation']
) as dag:

    task_transform = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_weather_data
    )