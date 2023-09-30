import os
from datetime import datetime, timedelta
from configparser import ConfigParser

import pandas as pd
import requests
import sqlalchemy as sa

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def get_and_store_data(base_url, endpoint_info, params=None):
    endpoint_url = f"{base_url}/{endpoint_info['endpoint']}"
    if params:
            endpoint_url += "?" + "&".join([f"{key}={value}" for key, value in params.items()])
    response = requests.get(endpoint_url)

    data = response.json()
    data = data["data"]
    df = pd.DataFrame(data)

    directory = os.path.dirname(endpoint_info['save_path'])
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    df.to_csv(endpoint_info['save_path'], index=False)

def connect_to_postgres(config_file_path="/opt/airflow/config.ini", section="postgres"):
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"El archivo de configuración '{config_file_path}' no existe.")

    config = ConfigParser()
    config.read(config_file_path)
    conn_data = config[section]

    host = conn_data.get("host")
    port = conn_data.get("port")
    db = conn_data.get("db")
    user = conn_data.get("user")
    pwd = conn_data.get("pwd")

    url = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

    conn = sa.create_engine(url)
    return conn

def load_data(data_path, table_name, engine):
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Fecha DATE PRIMARY KEY SORTKEY,
            Cotizacion_Dolar FLOAT,
            IPC_Gral FLOAT,
            Var_Porc_IPC_Gral FLOAT,
            IPC_Salud FLOAT,
            IPC_Educacion FLOAT,
            IPC_Alimentos_Bebidas FLOAT,
            IPC_Transporte FLOAT
        );
    """
    with engine.connect() as connection:
        connection.execute(create_table_sql)
    df = pd.read_csv(data_path, names=['Fecha', 'Cotizacion_Dolar', 'IPC_Gral', 'Var_Porc_IPC_Gral', 'IPC_Salud', 'IPC_Educacion', 'IPC_Alimentos_Bebidas', 'IPC_Transporte'])
    column_mapping = {
        'Fecha': 'Fecha',
        'Cotizacion_Dolar': 'Cotizacion_Dolar',
        'IPC_Gral': 'IPC_Gral',
        'Var_Porc_IPC_Gral': 'Var_Porc_IPC_Gral',
        'IPC_Salud': 'IPC_Salud',
        'IPC_Educacion': 'IPC_Educacion',
        'IPC_Alimentos_Bebidas': 'IPC_Alimentos_Bebidas',
        'IPC_Transporte': 'IPC_Transporte'
    }
    df = df.rename(columns=column_mapping)
    df.to_sql(table_name, engine, index=False, if_exists="append", method="multi")

def detect_anomalies(table_name, engine, max_value):
    query = f"""
        SELECT DISTINCT Fecha
        FROM {table_name}
        WHERE Cotizacion_Dolar > {max_value}
        """
    df = pd.read_sql_query(query, engine)
    dolar_alert=df.Fecha.values.tolist()
    print(f"Las fechas {dolar_alert} presentan cotizaciones de dólar anómalas.")

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 3, 
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id="alan_nahuel_vera_etl",
    schedule_interval="@monthly",
    default_args=default_args
)

base_url = 'https://apis.datos.gob.ar'

endpoints_info = [
    {
        "endpoint": "series/api/series?ids=92.1_TCV_0_0_21&header=titles&collapse=month&start_date=2020-01-01&sort=asc&format=json",
        "params": {
            "start_date": "{{ last_month.strftime('%Y-%m-1T%00:00:00Z') }}",
            "end_date": "{{ last_month.strftime('%Y-%m-31T%23:59:59Z') }}"
        },
        "save_path": "/opt/airflow/data/dolar/{{ ds }}/{{ execution_date.hour}}_hour.csv"
    },
    
    {
        "endpoint": "series/api/series?ids=101.1_I2NG_2016_M_22,101.1_I2NG_2016_M_22:percent_change,101.1_I2AMS_2016_M_30,101.1_I2ED_2016_M_13,101.1_I2AB_2016_M_26,101.1_I2TC_2016_M_19&start_date=2020-01-01&sort=asc&format=json",
        "params": {
            "start_date": "{{ last_month.strftime('%Y-%m-1T%00:00:00Z') }}",
            "end_date": "{{ last_month.strftime('%Y-%m-31T%23:59:59Z') }}"
        },
        "save_path": "/opt/airflow/data/dolar/{{ ds }}/{{ execution_date.hour}}_hour.csv"
        
    }
]

for idx, endpoint_info in enumerate(endpoints_info):
    last_month = datetime.now() - timedelta(days=30)

    endpoint_info["params"]["start_date"] = last_month.strftime('%Y-%m-01T00:00:00Z')
    endpoint_info["params"]["end_date"] = last_month.strftime('%Y-%m-31T23:59:59Z')

    get_data_task = PythonOperator(
        task_id=f"get_and_store_data_{idx}",
        python_callable=get_and_store_data,
        op_kwargs={
            "base_url": base_url,
            "endpoint_info": endpoint_info,
        },
        dag=dag
    )

    load_data_task = PythonOperator(
        task_id=f"load_data_{idx}",
        python_callable=load_data,
        op_kwargs={
            "data_path": endpoint_info['save_path'],
            "table_name": "hist_dolar_ipc",
            "engine": connect_to_postgres("/opt/airflow/config.ini"),
        },
        dag=dag
    )

    send_alert_task = PythonOperator(
        task_id=f"send_alert_{idx}",
        python_callable=detect_anomalies,
        op_kwargs={
            "table_name": "hist_dolar_ipc",
            "engine": connect_to_postgres("/opt/airflow/config.ini"),
            "max_value": 1000,
        },
        dag=dag
    )

    get_data_task >> load_data_task >> send_alert_task
