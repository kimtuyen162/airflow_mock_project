from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from python.get_path import source_file_path
from python.extract_data import extract_data
from python.transform_data import transform_data
from python.load_data import load_data
from python.verify_data import verify_data

default_args = {
    'owner':'airflow',
    'start_date':datetime(2024,8,4),
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15)
}

today = datetime.now()
today = today.strftime('%Y%m%d')

# Create a DAG object
with DAG(
    'filtering_customer_consumption_backup',
    default_args = default_args,
    schedule_interval = '50 11 * * *',
    catchup = False) as dag:
        
    # Check the existence of data from source
    check_data_existence = FileSensor(
        dag = dag,
        task_id = 'check_data_existence',
        filepath = source_file_path(today),
        fs_conn_id = 'data_source', # Connection ID to connect to the data source
        poke_interval = 300,
        timeout = 900
    )

    # Task to extract data from source in raw folder
    extract_task = extract_data(dag, today)

    # Task to transform data to get consumption data of alcoholic beverages, cereals bakery and meats poultry
    transform_task = transform_data(dag, today)

    # Task to load data to the destination
    load_task = load_data(dag, today)

    # Task to verify data in destination and metrics of DAG
    verify_task = verify_data(dag, today)

    trigger_metrics = TriggerDagRunOperator(
        task_id="trigger_metrics",
        trigger_dag_id="check_runtime",
        wait_for_completion=False,
        deferrable=False
    )
    # Set the task dependencies
    check_data_existence >> extract_task >> transform_task >> load_task >> verify_task >> trigger_metrics