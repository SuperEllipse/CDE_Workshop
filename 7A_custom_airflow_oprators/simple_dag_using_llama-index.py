from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import llama_index  # Importing llama-index library

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the Python function to be executed
def python_task():
    print("Running Python task")
    # Importing llama-index within the Python task
    import llama_index
    # Dummy operation with llama-index
    print(f"llama_index version: {llama_index.__version__}")

# Define the DAG
with DAG('simple_bash_python_dag', 
         default_args=default_args, 
         schedule_interval='@daily', 
        is_paused_upon_creation=False,         
         catchup=False) as dag:
    
    # Bash task
    bash_task = BashOperator(
        task_id='run_bash_command',
        bash_command='echo "Hello from Bash"'
    )

    # Python task
    python_task = PythonOperator(
        task_id='run_python_command',
        python_callable=python_task
    )

    # Set task dependencies
    bash_task >> python_task
