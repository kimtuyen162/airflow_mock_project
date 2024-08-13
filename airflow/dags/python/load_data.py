from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
from python.get_path import source_file_path, desination_file_path, db_file_path

# Function to save data to csv file
def _save_data_to_csv(today, **kwargs):
    filtered_data = pd.read_csv(desination_file_path('transformed', today), index_col = False)

    # Drop 'total_year_profit' column
    filtered_data = filtered_data.drop(columns = ['total_year_profit'])

    # Get data of each category
    alcoholic_data = filtered_data[filtered_data['category'] == 'Alcoholic beverages']
    cereals_data = filtered_data[filtered_data['category'] == 'Cereals and bakery products']
    meats_data = filtered_data[filtered_data['category'] == 'Meats and poultry']

    # Save data to csv file
    alcoholic_data.to_csv(desination_file_path('alcoholic', today), index = False)
    cereals_data.to_csv(desination_file_path('cereals_bakery', today), index = False)
    meats_data.to_csv(desination_file_path('meats_poultry', today), index = False)

    # Push data length of each category to XCom
    kwargs['ti'].xcom_push(key='alcoholic_data_length', value = len(alcoholic_data))
    kwargs['ti'].xcom_push(key='cereals_bakery_data_length', value = len(cereals_data))
    kwargs['ti'].xcom_push(key='meats_poultry_data_length', value = len(meats_data))
    
# Task function to save data to csv file and load data to postgresql
def load_data(dag, today):
    with TaskGroup ('load_data', dag = dag) as load_data:
        # PythonOperator to save data to csv file
        save_data_to_csv = PythonOperator(
            task_id = 'save_data_to_csv',
            python_callable = _save_data_to_csv,
            op_args = [today],
            provide_context = True
        )

        # Define parameters for PostgresOperator
        sql_params = [
            {'table_name': f'consumption_alcoholic_{today}', 'filepath': db_file_path('alcoholic', today)},
            {'table_name': f'consumption_cereals_bakery_{today}', 'filepath': db_file_path('cereals_bakery', today)},
            {'table_name': f'consumption_meats_poultry_{today}', 'filepath': db_file_path('meats_poultry', today)}
        ]

        # PostgresOperator to create table and insert data in postgresql
        save_data_to_db = PostgresOperator.partial(
            task_id = 'save_data_to_db',
            postgres_conn_id = 'postgres_database', # Connection ID to connect to the database
            sql = 'sql/sql_template.sql'
        ).expand(params = sql_params)
        
        # Set the task dependencies
        save_data_to_csv >> save_data_to_db

        return load_data