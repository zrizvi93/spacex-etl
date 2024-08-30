import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, LongType, TimestampType
import argparse

from config import CONFIG as Config
from constants import DATE_RANGE_TEMPLATE
from utils import File, parse_iso_format

spark = SparkSession.builder.appName("LaunchesDataExtraction").getOrCreate()
sc = spark.sparkContext

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", Config.aws_access_key)
hadoop_conf.set("fs.s3a.secret.key", Config.aws_secret_key)
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

class Launch:
    def __init__(self, data, day_utc):
        self.fairings = data.get("fairings")
        self.links = data.get("links")
        self.static_fire_date_utc =  datetime.strptime(data.get("static_fire_date_utc"), '%Y-%m-%dT%H:%M:%S.%fZ')
        self.static_fire_date_unix = data.get("static_fire_date_unix")
        self.net = data.get("net")
        self.window = data.get("window")
        self.rocket = data.get("rocket")
        self.success = data.get("success")
        self.failures = data.get("failures", [])
        self.details = data.get("details")
        self.crew = data.get("crew", [])
        self.ships = data.get("ships", [])
        self.capsules = data.get("capsules", [])
        self.payloads = data.get("payloads", [])
        self.launchpad = data.get("launchpad")
        self.flight_number = data.get("flight_number")
        self.name = data.get("name")
        self.date_utc = datetime.strptime(data.get("date_utc"), '%Y-%m-%dT%H:%M:%S.%fZ')
        self.date_unix = data.get("date_unix")
        self.date_local = parse_iso_format(data.get("date_local"))
        self.date_precision = data.get("date_precision")
        self.upcoming = data.get("upcoming")
        self.cores = data.get("cores", [])
        self.auto_update = data.get("auto_update")
        self.tbd = data.get("tbd")
        self.launch_library_id = data.get("launch_library_id")
        self.id = data.get("id")
        self.day_utc = day_utc
        
    def to_row(self):
        return Row({
            "fairings": self.fairings,
            "links": self.links,
            "static_fire_date_utc": self.static_fire_date_utc,
            "static_fire_date_unix": self.static_fire_date_unix,
            "net": self.net,
            "window": self.window,
            "rocket": self.rocket,
            "success": self.success,
            "failures": self.failures,
            "details": self.details,
            "crew": self.crew,
            "ships": self.ships,
            "capsules": self.capsules,
            "payloads": self.payloads,
            "launchpad": self.launchpad,
            "flight_number": self.flight_number,
            "name": self.name,
            "date_utc": self.date_utc,
            "date_unix": self.date_unix,
            "date_local": self.date_local,
            "date_precision": self.date_precision,
            "upcoming": self.upcoming,
            "cores": self.cores,
            "auto_update": self.auto_update,
            "tbd": self.tbd,
            "launch_library_id": self.launch_library_id,
            "id": self.id,
            "day_utc": self.day_utc
        })


def process_launches(start_date: str = "", incremental: bool = False):
  results = []
  http = requests.Session()
  http.mount("https://" , Config.adapter)
  http.mount("http://", Config.adapter)


  if not incremental:
    r = http.get("https://api.spacexdata.com/v4/launches")
    if not r.status_code == requests.codes.ok:
      r.raise_for_status()
    else:
       launches = r.json()
     
  else:
    launches = []
    current_page = 1
    limit = 10
    base_url = "https://api.spacexdata.com/v4/launches/query"
    # Convert the string to a datetime object
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = start_date_obj + timedelta(days=1)
    end_date = end_date_obj.strftime("%Y-%m-%d")

    while True:
      payload = json.loads(DATE_RANGE_TEMPLATE.substitute(start_date=start_date, end_date=end_date, current_page=current_page, limit=limit))
      r = http.post(base_url, json=payload)
      if not r.status_code == requests.codes.ok:
        r.raise_for_status()
      else:
        data = r.json()
        docs = data.get("docs", [])
        total_pages = data.get("totalPages", 1)
        has_next_page = data.get("hasNextPage", False)
        launches.extend(docs)
      if has_next_page and current_page < total_pages:
        current_page += 1
      else:
        break
  
  for l in launches:
    launch = Launch(l, start_date)
    results.append(launch.to_row())
    
  return results

def create_file(launches, start_date: str = ""):
  launch_schema = StructType([
      StructField("fairings", StringType(), nullable=True),
      StructField("links", StringType(), nullable=True),
      StructField("static_fire_date_utc", TimestampType(), nullable=True),
      StructField("static_fire_date_unix", LongType(), nullable=True),
      StructField("net", BooleanType(), nullable=True),
      StructField("window", IntegerType(), nullable=True),
      StructField("rocket", StringType(), nullable=True),
      StructField("success", BooleanType(), nullable=True),
      StructField("failures", ArrayType(StringType()), nullable=True),
      StructField("details", StringType(), nullable=True),
      StructField("crew", ArrayType(StringType()), nullable=True),
      StructField("ships", ArrayType(StringType()), nullable=True),
      StructField("capsules", ArrayType(StringType()), nullable=True),
      StructField("payloads", ArrayType(StringType()), nullable=True),
      StructField("launchpad", StringType(), nullable=True),
      StructField("flight_number", IntegerType(), nullable=True),
      StructField("name", StringType(), nullable=True),
      StructField("date_utc", TimestampType(), nullable=True),
      StructField("date_unix", LongType(), nullable=True),
      StructField("date_local", TimestampType(), nullable=True),
      StructField("date_precision", StringType(), nullable=True),
      StructField("upcoming", BooleanType(), nullable=True),
      StructField("cores", ArrayType(StringType()), nullable=True),
      StructField("auto_update", BooleanType(), nullable=True),
      StructField("tbd", BooleanType(), nullable=True),
      StructField("launch_library_id", StringType(), nullable=True),
      StructField("id", StringType(), nullable=False),
      StructField("day_utc", StringType(), nullable=True)
      ])
  
  file = File(spark, sc, result=launches[0], schema=launch_schema, s3loc="s3://anduril-takehome/launches/", overwrite_partition_where=start_date)
  return file


parser = argparse.ArgumentParser(description="Launches ETL job")
parser.add_argument("--start_date", required=True, help="The start date for the ETL job")
args = parser.parse_args()
start_date = args.start_date

# If we have an incremental batch job that writes this data to S3, we are going to upload the data in partitions. Sample code:
launches = process_launches(start_date = start_date, incremental = True)
file = create_file(launches)
df = file.dataframe
df.show()
# file.overwrite_partition()




  