
# Spark Installation 
`bin/spark_installation.sh`


# Local environmnet set up
`pyenv local`
`python3 -m venv spark-venv && source spark-venv/bin/activate`
`pip install -r requirements.txt`
`touch .env`
From project root, create environment variable file and add the following variables:
```
AIRFLOW_UID=33333
AIRFLOW_GID=0
AWS_ACCESS_KEY=YOURACCESSKEY
AWS_SECRET_KEY=YOURSECRETKEY
```

# Execute pyspark code directly
`python src/launches.py --start_date 2006-03-24 --env staging`
`python src/company.py --env staging`


ps aux | grep airflow and check if airflow webserver or airflow scheduler processes are running. 
If they are kill them and rerun using airflow scheduler -D


# Run airflow server
`airflow db init`
Initialize sqllite db airflow
`airflow db migrate`
`airflow users create --username airflow --firstname air --lastname flow --role Admin --email YOUREMAIL`

`airflow scheduler`
`airflow webserver --port 8080`

To kill any process on port 8080
`kill -9 $(lsof -i:8080 -t) 2> /dev/null`
Port 8793
`kill -9 $(lsof -i:8793 -t) 2> /dev/null`

# Running unit tests 
`bin/run-tests.sh`



# Connect dbt to postgres db in profiles.yml

`open ~/.dbt/profiles.yml`

```yaml
dbt_model:
outputs:
dev:
    dbname: swiftlydb
    host: 127.0.0.1
    pass: YOUR_PASSWORD_HERE
    port: 5432
    schema: public
    threads: 1
    type: postgres
    user: YOUR_USERNAME_HERE
target: dev
  ```