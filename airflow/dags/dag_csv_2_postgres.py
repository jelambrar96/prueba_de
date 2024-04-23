import os
from datetime import datetime 

import pandas as pd
import sqlalchemy as sqla

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

DEFAULT_PATH = "/tmp/data/" # from docker compose

DB_POSTGRES_USER = os.environ.get('DB_POSTGRES_USER')
DB_POSTGRES_PASSWORD = os.environ.get('DB_POSTGRES_PASSWORD')
DB_POSTGRES_DB = os.environ.get('DB_POSTGRES_DB')
DB_POSTGRES_HOST = "db" # taken from  docker compose
DB_POSTGRES_PORT = 5432 # taken from  docker compose


def create_filepath(path, ds):
    ds_date = datetime.strptime(ds, "%Y-%m-%d")
    ds_date_final = ds_date.strftime("%Y-%m.csv").replace("-0", "-") # remove zero padding
    return os.path.join(path, ds_date_final)


def function_migrate_data(path, ds):
    # 
    sql_uri = "postgresql://${DB_POSTGRES_USER}:${DB_POSTGRES_PASSWORD}@${DB_POSTGRES_HOST}:${DB_POSTGRES_PORT}/${DB_POSTGRES_DB}"
    engine = sqla.create_engine(sql_uri)
    # 
    filepath = create_filepath(path, ds)
    df = pd.read_csv(filepath, index_col=False)
    df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=False) # .df.strftime('%m/%d/%Y')
    df.to_sql("csv_table", con=engine, if_exists='append')
    # 
    ds_date = datetime.strptime(ds, "%Y-%m-%d")
    describe_df = df.describe().reset_index().rename(columns={'index': 'statistic'})
    describe_df['timestamp'] = ds_date
    df.to_sql("statistics", con=engine, if_exists='append')


dag_csv_2_postgres = DAG(
    dag_id="dag_csv_2_postgres",
    schedule_interval="@monthly",
    start_date=datetime(year=2012, month=1, day=1)
)


task_check_file = FileSensor(
    task_id="task_check_file",
    path=create_filepath(DEFAULT_PATH, '{{ ds }}')
)

task_migrate_data = PythonOperator(
    task_id="task_migrate_data",
    dag=dag_csv_2_postgres,
    python_callable=function_migrate_data,
    op_kwags={
        "path": DEFAULT_PATH,
        "ds": '{{ ds }}'
    }
)

task_check_file >> task_migrate_data