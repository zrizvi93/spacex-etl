import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when
from datetime import timedelta, datetime
import argparse

from config import CONFIG as Config
from utils import File

spark = SparkSession.builder.appName("CompanyDataExtraction").getOrCreate()
sc = spark.sparkContext

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", Config.aws_access_key)
hadoop_conf.set("fs.s3a.secret.key", Config.aws_secret_key)
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

parser = argparse.ArgumentParser(description="Launches Data quality check")
parser.add_argument("--s3loc", required=True, help="The data to check")
parser.add_argument("--task", required=True, help="The check to run")
args = parser.parse_args()
s3loc = args.s3loc
validate = args.validate

def read_data(s3loc: str) -> DataFrame:
   df = spark.read.parquet(s3loc, header=True, inferSchema=True)
   return df

def dupe_check(df: DataFrame) -> bool:
   result = True
   duplicate_check = df.groupBy("id").count().where(col("count") > 1)
   if duplicate_check.count() > 0:
      print("Duplicate ID check failed")
      result = False
   return result

function_map = {
    "dupe_check": dupe_check
}

df = read_data(s3loc)
if validate in function_map:
   result = function_map[validate](df)
   if not result:
      raise ValueError("Data quality check failed")
print("Data quality checks success")
