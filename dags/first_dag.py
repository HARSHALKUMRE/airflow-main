from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

def first_fuction_execute(**context):
    print("first function execute")
    context['ti'].xcom_push(key='mykey',value='first_function_execute say hello')
    
def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key='mykey')
    print("i am in second function execute got value : {} from function 1".format(instance))

with DAG(
    dag_id = 'first_dag',
    schedule_interval = '@daily',
    default_args = {
        "owner":"airflow",
        "retries":1,
        "retry_delay":timedelta(minutes=5),
        "start_date":datetime(2023,3,20),
    },
    catchup=False
) as f:
    first_function_execute = PythonOperator(
        task_id = 'first_fuction_execute',
        python_callable = first_fuction_execute,
        op_kwargs = {"name":"Harshal"}
    )
    second_function_execute = PythonOperator(
        task_id = 'second_function_execute',
        python_callable = second_function_execute,
        provide_context = True
    )
    
first_function_execute>>second_function_execute
    