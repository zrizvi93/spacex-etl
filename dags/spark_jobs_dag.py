import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    task_id='ships_staging_spark_submit',
    dag=spark_dag,
    application_args=["--env", "staging"]
)

launches_staging_task = SparkSubmitOperator(
    application='../src/launches.py',
    conn_id='spark_local',
    task_id='launches_staging_spark_submit',
    dag=spark_dag,
    application_args=["--start_date", "{{ ds }}",
                      "--env", "staging"]
)

landpads_staging_task = SparkSubmitOperator(
    application='../src/landpads.py',
    conn_id='spark_local',
    task_id='landpads_staging_spark_submit',
    dag=spark_dag,
    application_args=["--env", "staging"]
)

company_staging_task = SparkSubmitOperator(
    application='../src/company',
    conn_id= 'spark_local', 
    task_id='company_staging_spark_submit', 
    dag=spark_dag,
    application_args=["--env", "staging"]
    )

staging_quality_check_task = SparkSubmitOperator(
    application='../src/dataquality_checks.py',
    conn_id='spark_local',
    task_id='staging_dq_check',
    application_args=["--s3loc", "s3://anduril-takehome/staging/company/",
                      "--validate", "dupe_check"]
)

ships_prod_task = SparkSubmitOperator(
    application='../src/ships.py',
    conn_id='spark_local',
    task_id='ships_prod_spark_submit',
    dag=spark_dag,
    application_args=["--env", "prod"]
)

launches_prod_task = SparkSubmitOperator(
    application='../src/launches.py',
    conn_id='spark_local',
    task_id='launches_prod_spark_submit',
    dag=spark_dag,
    application_args=["--start_date", "{{ ds }}",
                      "--env", "prod"]
)

landpads_prod_task = SparkSubmitOperator(
    application='../src/landpads.py',
    conn_id='spark_local',
    task_id='landpads_prod_spark_submit',
    dag=spark_dag,
    application_args=["--env", "prod"]
)

company_prod_task = SparkSubmitOperator(
    application='../src/company',
    conn_id= 'spark_local', 
    task_id='company_prod_spark_submit', 
    dag=spark_dag,
    application_args=["--env", "prod"]
)


# Run staging tasks in parallel
[ships_staging_task, launches_staging_task, landpads_staging_task, company_staging_task] >> staging_quality_check_task

# Run production tasks only if data quality checks pass
staging_quality_check_task >> [ships_prod_task, launches_prod_task, landpads_prod_task, company_prod_task]

