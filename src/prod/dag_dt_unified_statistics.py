# import libraries
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# define the default_args

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# programmer specific settings

dag_python = DAG(
    dag_id="dag_dt_unified_statistics_v3",
    default_args=default_args,
    schedule_interval='30 04 * * *',
#    dagrun_timeout=timedelta(minutes=5),
    description='extract data UnifiedStatistics',
    start_date=airflow.utils.dates.days_ago(1)
    )

# execute the command

task1 = BashOperator(
    task_id='dt_extract_data_bd',
    bash_command='/home/pydev/pyenv/venvdt/bin/python3 /home/pydev/workflow/dt_unifed_statistics/src/prod/extract_data_db.py',
    dag=dag_python)

task2 = BashOperator(
    task_id='dt_transform_unified_statatics',
    bash_command='/home/pydev/pyenv/venvdt/bin/python3 /home/pydev/workflow/dt_unifed_statistics/src/prod/visitas_vendedores_v2.py',
    dag=dag_python)

task4 = BashOperator(
    task_id='dt_delete_data_UnifiedStatistics_to_folder_server',
    bash_command='yes | rm -rf /mnt/z/26.Data_Model/9.Misc/3.Tablefiles/resultUnifiedStatistics',
    dag=dag_python)

task5 = BashOperator(
    task_id='dt_copy_data_UnifiedStatistics_to_folder_server',
    bash_command='yes | cp -rf /home/pydev/workflow/dt_unifed_statistics/resultUnifiedStatistics/ /mnt/z/26.Data_Model/9.Misc/3.Tablefiles/',
    dag=dag_python)   

# task programmer
task1 >> task2  >> task4 >> task5