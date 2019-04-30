from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 30),
    'email': ['dmitrii_kanaaev@epam.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('spark_workflow', default_args=default_args, schedule_interval=timedelta(minutes=10))

sparkSubmit = """
    echo "Script has been started"
    spark-submit \
    --class "{{ params.class }}" \
    --master "{{ params.master }}" \
    --deploy-mode "{{ params.deployMode }}" \
    "{{ params.jarPath }}" \
    "{{ params.sourceFile }}" \
    "{{ params.targetPath }}"
"""

hiveCreateTable = """
    beeline -u jdbc:hive2://sandbox.hortonworks.com:10000/default -n root -p root --silent=true -e \
    "CREATE EXTERNAL TABLE IF NOT EXISTS second_task_result \
    ( \
        hotel_continent INT, \
        hotel_country INT, \
        hotel_market INT, \
        \`_c3\` INT) \
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' \
    LOCATION '/user/raj_ops/spark-result'"
"""

t1 = BashOperator(
    task_id='run_spark',
    bash_command=sparkSubmit,
    params={'class': 'com.epam.hadoopcourse.homework.spark.Application',
            'master': 'yarn',
            'deployMode': 'client',
            'jarPath': '/var/workflow/spark-task1-1.0-SNAPSHOT.jar',
            'sourceFile': 'hdfs://sandbox.hortonworks.com:8020/user/raj_ops/train.parquet/',
            'targetPath': 'hdfs://sandbox.hortonworks.com:8020/user/raj_ops/spark-result/'},
    dag=dag)

t2 = BashOperator(
    task_id='create_hive_ext_table',
    bash_command=hiveCreateTable,
    dag=dag)

t2.set_upstream(t1)
