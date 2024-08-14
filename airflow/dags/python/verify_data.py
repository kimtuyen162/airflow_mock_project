from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.models.dagrun import DagRun
from datetime import datetime
import pandas as pd
from python.get_path import desination_file_path

def calculate_runtime_taskgroup(dag_name, task_group):
    dag_run = DagRun.find(dag_id = dag_name)[-1] # Get the lastest DagRun
    list_task = dag_run.get_task_instances()

    task_group_ti = [
        ti for ti in list_task if ti.task_id.startswith(task_group + '.') or ti.task_id.startswith(task_group)
    ]

    start_time = min(ti.start_date for ti in task_group_ti if ti.start_date)
    end_time =max(ti.end_date for ti in task_group_ti if ti.end_date)

    duration = (end_time - start_time).total_seconds()

    return duration

def _check_runtime(dag_name):
    dag_run = DagRun.find(dag_id = dag_name)[-1] # Get the latest DagRun

    # Print out start and end of DAG
    print(f'DAG: {dag_name}.\nStart: {dag_run.start_date}.\nEnd: {dag_run.end_date}')

    # List of task group in DAG
    list_task_group = ['check_data_existence', 'extract_specific_data', 'transform_data', 'load_data', 'verify_data']
    
    # List to store task_id and its duration
    data = []

    for task_group in list_task_group:
        data.append({
            'task_id': task_group,
            'duration': calculate_runtime_taskgroup(dag_name, task_group)
        })
    
    duration_df = pd.DataFrame(data)

    longest_runtime_task = duration_df.loc[duration_df['duration'].idxmax()]

    shortest_runtime_task = duration_df.loc[duration_df['duration'].idxmin()]

    print(f'Task run longest: {longest_runtime_task['task_id']}\nDuration {longest_runtime_task['duration']}s')

    print(f'Task run shortest: {shortest_runtime_task['task_id']}\nDuration {shortest_runtime_task['duration']}s')

    # Save the metrics to a file
    today = datetime.now()
    today = today.strftime('%Y%m%d')
    with open(desination_file_path('verify_data', today), 'a') as f:
        f.write(f'\nMetrics of DAG {today}\n')
        f.write(f'DAG: {dag_name}.\n Start: {dag_run.start_date}.\n End: {dag_run.end_date}\n')
        f.write(f'Task run longest: {longest_runtime_task['task_id']} with duration {longest_runtime_task['duration']}s\n')
        f.write(f'Task run shortest: {shortest_runtime_task['task_id']} with duration {shortest_runtime_task['duration']}s\n')
    
    f.close()

def number_of_new_records(table_name):
    hook = PostgresHook(postgres_conn_id = 'postgres_database')
    number_of_records = hook.get_records(f"""SELECT COUNT(*) FROM {table_name}""")

    return number_of_records[0][0]

def _check_data_loss(today, **kwargs):
    alcoholic_data_length = kwargs['ti'].xcom_pull(key = 'alcoholic_data_length', task_ids = 'load_data.save_data_to_csv')
    cereals_data_length = kwargs['ti'].xcom_pull(key = 'cereals_bakery_data_length', task_ids = 'load_data.save_data_to_csv')
    meats_data_length = kwargs['ti'].xcom_pull(key = 'meats_poultry_data_length', task_ids = 'load_data.save_data_to_csv')

    table_name = [f'consumption_alcoholic_{today}', f'consumption_cereals_bakery_{today}', f'consumption_meats_poultry_{today}']
    
    print(f'Number of records in {table_name[0]} table (source/destination): {alcoholic_data_length}/{number_of_new_records(table_name[0])}')
    print(f'Number of records in {table_name[1]} table (source/destination): {cereals_data_length}/{number_of_new_records(table_name[1])}')
    print(f'Number of records in {table_name[2]} table (source/destination): {meats_data_length}/{number_of_new_records(table_name[2])}')

    # Save the metrics to a file
    with open(desination_file_path('verify_data', today), 'a') as f:
        f.write(f'\nVerify data loss {today}\n')
        f.write(f'Number of records in {table_name[0]} table (source/destination): {alcoholic_data_length}/{number_of_new_records(table_name[0])}\n')
        f.write(f'Number of records in {table_name[1]} table (source/destination): {cereals_data_length}/{number_of_new_records(table_name[1])}\n')
        f.write(f'Number of records in {table_name[2]} table (source/destination): {meats_data_length}/{number_of_new_records(table_name[2])}\n')
    
    f.close()

def _export_data_to_csv(table_name):
    hook = PostgresHook(postgres_conn_id = 'postgres_database')
    data = hook.get_pandas_df(f"""SELECT * FROM {table_name}""")
    data.to_csv(f'./resource/data/results/{table_name}.csv', index = False)

def verify_data(dag, today):
    with TaskGroup('verify_data', dag = dag) as verify_data:
        check_data_loss = PythonOperator(
            task_id = 'check_data_loss',
            python_callable = _check_data_loss,
            provide_context = True,
            op_args = [today]
        )

        export_data_to_csv = PythonOperator.partial(
            task_id = 'export_data_to_csv',
            python_callable = _export_data_to_csv
        ).expand_kwargs(
            [
                {'op_kwargs': {'table_name': f'consumption_alcoholic_{today}'}},
                {'op_kwargs': {'table_name': f'consumption_cereals_bakery_{today}'}},
                {'op_kwargs': {'table_name': f'consumption_meats_poultry_{today}'}}
            ]
        )

        [check_data_loss, export_data_to_csv]
    return verify_data