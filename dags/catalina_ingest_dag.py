from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Define your dags settings
default_args = {
    'owner': 'Tom',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 8),  # Change this to your desired start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define your custom Python function to run 'ingest.py'
def run_ingest_script():
    try:
        # Change directory to where 'ingest.py' is located
        os.chdir('/Users/tomotto/PycharmProjects/ExampleIngest/ingest.py')

        # Run 'ingest.py'
        os.system('python ingest.py')
    except Exception as e:
        raise Exception(f"Error running ingest.py: {str(e)}")


# Create the dags
dag = DAG(
    'ingest_data_dag',
    default_args=default_args,
    description='dags to run ingest.py periodically',
    schedule_interval=timedelta(days=1),  # Adjust the schedule interval as needed
    catchup=False,
)

# Define the PythonOperator to run 'ingest.py'
run_ingest_task = PythonOperator(
    task_id='run_ingest_task',
    python_callable=run_ingest_script,
    dag=dag,
)

# Set the task dependencies if needed
# Example: run_ingest_task.set_upstream(...)

if __name__ == "__main__":
    dag.cli()
