import os
import logging
from datetime import datetime 

import pandas as pd
import sqlalchemy as sqla

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from utils import function_check_file
from utils import function_metric_data
from utils import function_migrate_data


DEFAULT_PATH = "/tmp/data/" # from docker compose

DB_POSTGRES_USER = os.environ.get('DB_POSTGRES_USER')
DB_POSTGRES_PASSWORD = os.environ.get('DB_POSTGRES_PASSWORD')
DB_POSTGRES_DB = os.environ.get('DB_POSTGRES_DB')
DB_POSTGRES_HOST = "db" # taken from  docker compose
DB_POSTGRES_PORT = 5432 # taken from  docker compose

PRICES_TABLE = "prices"
METRTRIC_TABLE = "price_metrics"
DIM_METRIC_TABLE = "dim_metrics"


dag_csv_2_postgres = DAG(
    dag_id="dag_csv_2_postgres",
    schedule_interval="@monthly",
    start_date=datetime(year=2012, month=1, day=1),
    end_date=datetime(year=2012, month=5, day=1)
)

task_start = DummyOperator(
    task_id="task_start",
    dag=dag_csv_2_postgres
)


task_check_file = PythonSensor(
    task_id="task_check_file",
    python_callable=function_check_file,
    op_kwargs={
        "path": DEFAULT_PATH,
        "ds": '{{ ds }}'
    },
    dag=dag_csv_2_postgres
)

task_migrate_data = PythonOperator(
    task_id="task_migrate_data",
    dag=dag_csv_2_postgres,
    python_callable=function_migrate_data,
    op_kwargs={
        "path": DEFAULT_PATH,
        "ds": '{{ ds }}'
    }
)

task_metric_data = PythonOperator(
    task_id="task_metric_data",
    dag=dag_csv_2_postgres,
    python_callable=function_metric_data,
    op_kwargs={
        "path": DEFAULT_PATH,
        "ds": '{{ ds }}'
    }
)


task_start >> task_check_file >> task_migrate_data >> task_metric_data

