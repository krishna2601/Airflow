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
        'email': ['myemail@xyz.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
}

#3.  Define DAG
#dag_id : id of the DAG which you will see on web console
#default_args: passing the dictionary that we created in above step(it's 
like settings for this DAG)
#description : helps us in understanding what this DAG will do
#schedule_interval: provides information on how frequently this DAG will 
run
dag =  DAG(
        dag_id = 'my-first-dag',
        default_args = default_args,
        description = 'My first DAG',
        schedule_interval = timedelta(days=1)
)

#4. Define the tasks

#task_id : identifies the task
#bash_command:  what command it represents
#dag: which dag task it belongs to

#define the first task named extract
extract = BashOperator(
        task_id = 'extract',
        bash_command='cut -d":" -f1,3,6 /etc/passwd > 
/home/project/airflow/dags/extracted-data.txt',
        dag = dag
)

#define the second task named transform
transform_and_load = BashOperator(
        task_id = 'transform',
        bash_command = 'tr ":" "," < 
/home/project/airflow/dags/extracted-data.txt > 
/home/project/airflow/dags/transformed-data.csv',
        dag=dag
)

#5. task pipeline(here task extract must run first, followed by transform, 
followed by Load)
extract >> transform_and_load


#below steps can be performed using terminal
#copy DAG python file into dags directory in the AIRFLOW_HOME directory.
#cp Airflow_DAG_BashOperator.py
#To verify DAG actually got submitted. Run below command.
# airflow dags list
# airflow dags list|grep "Airflow_DAG_BashOPerator1.py"
#  airflow tasks list Airflow_DAG_BashOPerator1.py"
