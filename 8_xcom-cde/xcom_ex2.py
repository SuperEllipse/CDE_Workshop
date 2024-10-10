from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json

# Define a function that will extract and print the fact and length from the JSON response
def print_cat_fact(ti):
    # Get the output of the BashOperator
    bash_output = ti.xcom_pull(task_ids='get_cat_fact')
    # Load the JSON response
    fact_data = json.loads(bash_output)
    # Extract and print the fact and its length
    fact = fact_data['fact']
    length = fact_data['length']
    print(f"Cat Fact: {fact}")
    print(f"Fact Length: {length}")

# Define the DAG
default_args = {
    'owner': 'airflow',
}

with DAG(
    'cat_fact_dag_example',
    default_args=default_args,
    description='A simple DAG to fetch and print cat facts',
    schedule_interval=None,  # Set this to None to trigger manually
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    # BashOperator to fetch a cat fact
    get_cat_fact = BashOperator(
        task_id='get_cat_fact',
        bash_command="curl -s https://catfact.ninja/fact",
    )

    # PythonOperator to print the fact and its length
    print_fact = PythonOperator(
        task_id='print_fact',
        python_callable=print_cat_fact,
    )

    # Task dependencies
    get_cat_fact >> print_fact

