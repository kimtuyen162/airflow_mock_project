from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
from python.verify_data import _check_runtime

default_args = {
    'owner':'airflow',
    'start_date':datetime(2024,8,4),
    'depends_on_past': False
}

dag = DAG('check_runtime', default_args = default_args, schedule_interval = None, catchup = False)

check_runtime = PythonOperator(
    task_id = 'check_runtime',
    dag = dag,
    python_callable = _check_runtime,
    op_args = ['filtering_customer_consumption_backup']
)

check_runtime