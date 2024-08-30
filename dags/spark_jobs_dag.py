import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'zrizvi',    
    'retry_delay': timedelta(minutes=5),
}

spark_dag = DAG(
        dag_id = "spark_airflow_dag",
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,	
        dagrun_timeout=timedelta(minutes=60),
        description='use case of SparkSubmitOperator in airflow',
        start_date = days_ago(1)
)

ships_staging_task = SparkSubmitOperator(
    application='../src/ships.py',
    conn_id='spark_local',
    task_id='ships_spark_submit',
    dag=spark_dag
)

launches_staging_task = SparkSubmitOperator(
    application='../src/launches.py',
    conn_id='spark_local',
    task_id='launches_spark_submit',
    dag=spark_dag,
    application_args=["--start_date", "{{ ds }}"]
)

landpads_staging_task = SparkSubmitOperator(
    application='../src/landpads.py',
    conn_id='spark_local',
    task_id='landpads_spark_submit',
    dag=spark_dag
)

company_staging_task = SparkSubmitOperator(
    application ='../src/company',
    conn_id= 'spark_local', 
    task_id='company_spark_submit', 
    dag=spark_dag
    )

# Dummy data quality check function
def check_data_quality(**kwargs):
    print("Performing data quality checks...")
    return True

# Check on Staging Data
quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=spark_dag
)

ships_prod_task = SparkSubmitOperator(
    application='../src/ships.py',
    conn_id='spark_local',
    task_id='ships_spark_submit',
    dag=spark_dag
)

launches_prod_task = SparkSubmitOperator(
    application='../src/launches.py',
    conn_id='spark_local',
    task_id='launches_spark_submit',
    dag=spark_dag,
    application_args=["--start_date", "{{ ds }}"]
)

landpads_prod_task = SparkSubmitOperator(
    application='../src/landpads.py',
    conn_id='spark_local',
    task_id='landpads_spark_submit',
    dag=spark_dag
)

company_prod_task = SparkSubmitOperator(
    application ='../src/company',
    conn_id= 'spark_local', 
    task_id='company_spark_submit', 
    dag=spark_dag
    )


# Run staging tasks in parallel
[ships_staging_task, launches_staging_task, landpads_staging_task, company_staging_task] >> quality_check_task

# Run production tasks only if data quality checks pass
quality_check_task >> [ships_prod_task, launches_prod_task, landpads_prod_task, company_prod_task]

