from airflow import DAG
from airflow.utils.dates import days_ago
from custom_airflow_operator_superellipse.arithmetic_operator import ArithmeticOperator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'arithmetic_operations_dag1',
    default_args=default_args,
    description='A simple DAG to perform arithmetic operations using custom operator',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
    is_paused_upon_creation=False,
)

add_task = ArithmeticOperator(
    task_id='addition_task',
    num1=10,
    num2=5,
    operation='add',
    dag=dag,
)

subtract_task = ArithmeticOperator(
    task_id='subtraction_task',
    num1=10,
    num2=5,
    operation='subtract',
    dag=dag,
)

multiply_task = ArithmeticOperator(
    task_id='multiplication_task',
    num1=10,
    num2=5,
    operation='multiply',
    dag=dag,
)

divide_task = ArithmeticOperator(
    task_id='division_task',
    num1=10,
    num2=5,
    operation='divide',
    dag=dag,
)

add_task >> subtract_task >> multiply_task >> divide_task