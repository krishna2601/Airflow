#An apache airflow DAG is a python program. 
#1. import the libraries

from datetime import timedelta
#the DAG object, it will be needed to instantiate a DAG
from airflow import DAG 
#operators to write DAG tasks
from airflow.operators.bash_operator import BashOperator 
#to schedule airflow pipeline
from airflow.utils.dates import days_ago

#2. Define DAG arguements
default_args = {
    'owner': 'me',
    'start_date': days_ago(0),
    'email': ['xyz@myemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#3.  Define DAG
#dag_id : id of the DAG which you will see on web console
#default_args: passing the dictionary that we created in above step(it's 
like settings for this DAG)
#description : helps us in understanding what this DAG will do
#schedule_interval: provides information on how frequently this DAG will 
run
dag =  DAG(
        dag_id = 'sample-etl-dag'
        default_args = degault_args,
        description = 'Sample ETL DAG using Bash',
        schedule_interval = timedelta(days=1)
)

#4. Define the tasks

#task_id : identifies the task
#bash_command:  what command it represents
#dag: which dag task it belongs to

#define the first task named extract
extract = BashOperator(
        task_id = 'extract',
        bash_command=' echo "extract"',
        dag = dag
)

#define the second task named transform
transform = BashOperator(
        task_id = 'transform',
        bash_command = 'echo "Transform",
        dag=dag
)

#define the third task named load
load = BashOperator(
        task_id = 'load',
        bash_command = 'echo "Load"',
        dag = dag
)

#5. task pipeline(here task extract must run first, followed by transform, 
followed by Load)
extract >> transform >> load
