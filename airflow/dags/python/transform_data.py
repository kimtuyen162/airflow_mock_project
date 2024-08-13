from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
from python.get_path import source_file_path, desination_file_path

# Function to format 'Month' column to 'YYYY/MM/DD' format and change name of columns then save to filtered folder
def _format_date(today):
    df = pd.read_csv(source_file_path(today))
    df['Month'] = pd.to_datetime(df["Month"]).dt.strftime("%Y/%m/%d")
    df.to_csv(source_file_path(today), index = False)

    df['pipeline_exc_datetime'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    df.columns = ['category', 'sub_category', 'aggregation_date', 'millions_of_dollar', 'pipeline_exc_datetime']

    df.to_csv(desination_file_path('filtered', today), index = False)

# Function to get top 3 consumption of each category and sub_category in each year
def _top3_consumption(today):
    source_data = pd.read_csv(desination_file_path('filtered', today))

    # Convert 'aggregation_date' to datetime
    source_data['aggregation_date'] = pd.to_datetime(source_data['aggregation_date'])

    # Create 'year' column
    source_data['year'] = source_data['aggregation_date'].dt.year

    # Create 'total_year_profit' column for each category, sub_category and year
    source_data['total_year_profit'] = source_data.groupby(['year', 'sub_category', 'category'])['millions_of_dollar'].transform('sum')

    # Get top 3 profit of each category, sub_category and year
    source_data_top3_profit = source_data.groupby(['category', 'sub_category', 'year'], group_keys = False).apply(
        lambda top3: top3.nlargest(3, 'millions_of_dollar')
    )

    # Sort data by 'total_year_profit', 'category', 'sub_category' and 'millions_of_dollar'
    source_data_top3_profit = source_data_top3_profit.sort_values(
        by=['total_year_profit', 'category', 'sub_category', 'millions_of_dollar'],
        ascending=[False, True, True, False]
        )
    
    source_data_top3_profit.reset_index(drop = True, inplace = True)

    # Save data to transformed folder
    source_data_top3_profit = source_data_top3_profit[['category', 'sub_category', 'aggregation_date', 'millions_of_dollar', 'pipeline_exc_datetime', 'total_year_profit']]
    source_data_top3_profit.to_csv(desination_file_path('transformed', today), index = False)

# Task function to transform data
def transform_data(dag, today):
    with TaskGroup('transform_data', dag = dag) as transform_data:
        # PythonOperator to format 'Month' column to 'YYYY/MM/DD' format and change name of columns then save to filtered
        format_date = PythonOperator(
            task_id = 'format_date',
            python_callable = _format_date,
            op_args = [today]
        )

        # PythonOperator to get top 3 consumption of each category and sub_category in each year
        top3_consumption = PythonOperator(
            task_id = 'top3_consumption',
            python_callable = _top3_consumption,
            op_args = [today]
        )

        # Set the task dependencies
        format_date >> top3_consumption

    return transform_data