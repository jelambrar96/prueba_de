import os
from datetime import datetime


from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor


from utils import function_check_file
from utils import function_metric_data
from utils import function_migrate_data

DEFAULT_PATH = "/tmp/data/" # from docker compose


dag_validation_csv_2_postgres = DAG(
    dag_id="dag_validation_csv_2_postgres",
    schedule_interval=None,
    start_date=datetime.now(),
    catchup=False,
)

task_start_validation = DummyOperator(
    task_id="task_start_validation",
    dag=dag_validation_csv_2_postgres
)

task_check_file_validation = PythonSensor(
    task_id="task_check_file_validation",
    python_callable=function_check_file,
    op_kwargs={
        "path": f"{DEFAULT_PATH}/validation.csv",
        "ds": None
    },
    dag=dag_validation_csv_2_postgres
)

task_migrate_data_validation = PythonOperator(
    task_id="task_migrate_data_validation",
    dag=dag_validation_csv_2_postgres,
    python_callable=function_migrate_data,
    op_kwargs={
        "path": f"{DEFAULT_PATH}/validation.csv",
        "ds": None
    }
)

task_metric_data_validation = PythonOperator(
    task_id="task_metric_data_validation",
    dag=dag_validation_csv_2_postgres,
    python_callable=function_metric_data,
    op_kwargs={
        "path": f"{DEFAULT_PATH}/validation.csv",
        "ds": None
    }
)

task_start_validation >> task_check_file_validation >> task_migrate_data_validation >> task_metric_data_validation
