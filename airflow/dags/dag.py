import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10),
    'email': ['test@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'batch_line',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
MONGO_URI = "mongodb://mongodb:27017"
pyspark_app_home = "/home/scripts"

# b9 is not included due to a NumPy incompatibility issue, so I run it manually
scripts = [f"b{i}.py" for i in range(1, 11) if i != 9]

# define tasks b1, b2, b3, b4... (run in sequence)
tasks = {}
task_ids = []
for script in scripts:
    task_id = script.replace(".py","")
    task_ids.append(task_id)
    tasks[task_id] = BashOperator(
        task_id=task_id,
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 "
            f"{pyspark_app_home}/{script}"
        ),
        env={
            "CORE_CONF_fs_defaultFS": HDFS_NAMENODE,
            "MONGO_URI": MONGO_URI
        },
        dag=dag
    )

for a, b in zip(task_ids, task_ids[1:]):
    tasks[a] >> tasks[b]