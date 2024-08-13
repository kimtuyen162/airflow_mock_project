from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from python.get_path import source_file_path, desination_file_path

# Function to extract specific data from consumption file in raw folder
def _extract_specific_data(today):
    
    df = pd.read_csv(source_file_path(today), index_col = False)

    list_products = ['Alcoholic beverages',  'Cereals and bakery products', 'Meats and poultry']
    filtered_data = df[df['Category'].isin(list_products)]

    filtered_data.to_csv(desination_file_path('filtered', today), index = False)

# Task function to extract specific data from consumption file in raw folder
def extract_data(dag, today):
    extract_specific_data = PythonOperator(
        task_id = 'extract_specific_data',
        dag = dag,
        python_callable = _extract_specific_data,
        op_args = [today]
    )

    extract_specific_data

    return extract_specific_data