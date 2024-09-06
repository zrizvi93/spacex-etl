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

parser = argparse.ArgumentParser(description="Launches ETL job")
parser.add_argument("--env", required=True, help="The environment")
args = parser.parse_args()
env = args.env

class Company:
   def __init__(self, data):
      self.hq_addr = data['headquarters'].get('address', '')
      self.hq_city = data['headquarters'].get('city', '')
      self.hq_state = data['headquarters'].get('state', '')
      self.website = data['links'].get('website', '')
      self.flickr = data['links'].get('flickr', '')
      self.twitter = data['links'].get('twitter', '')
      self.elon_twitter = data['links'].get('elon_twitter', '')
      self.name = data.get('name', "")
      self.founder = data.get('founder', "")
      self.founded = data.get('founded', 0)
      self.employees = data.get('employees', 0)
      self.vehicles = data.get('vehicles', 0)
      self.launch_sites = data.get('launch_sites', 0)
      self.test_sites = data.get('test_sites', 0)
      self.ceo = data.get('ceo', "")
      self.cto = data.get('cto', "")
      self.coo = data.get('coo', "")
      self.cto_propulsion = data.get('cto_propulsion', "")
      self.valuation = data.get('valuation', 0)
      self.summary = data.get('summary', "")
      self.id = data.get('id', "")

   def to_dict(self):
      return {
         "hq_addr": self.hq_addr,
         "hq_city": self.hq_city,
         "hq_state": self.hq_state,
         "website": self.website,
         "flickr": self.flickr,
         "twitter": self.twitter,
         "elon_twitter": self.elon_twitter,
         "name": self.name,
         "founder": self.founder,
         "founded": self.founded,
         "employees": self.employees,
         "vehicles": self.vehicles,
         "launch_sites": self.launch_sites,
         "test_sites": self.test_sites,
         "ceo": self.ceo,
         "cto": self.cto,
         "coo": self.coo,
         "cto_propulsion": self.cto_propulsion,
         "valuation": self.valuation,
         "summary": self.summary,
         "id": self.id
      }


def process_company():
  http = requests.Session()
  http.mount("https://" , Config.adapter)
  http.mount("http://", Config.adapter)
  r = http.get("https://api.spacexdata.com/v4/company")
  if r.status_code == requests.codes.ok:
     company_data = r.json()
     company = Company(company_data)
  else:
     r.raise_for_status()
  return company

def dupe_check(df: DataFrame) -> bool:
   result = True
   duplicate_check = df.groupBy("id").count().where(col("count") > 1)
   if duplicate_check.count() > 0:
      print("Duplicate ID check failed")
      result = False
   return result

def create_file(company, env):
   schema = StructType([
        StructField("name", StringType(), True),
        StructField("founder", StringType(), True),
        StructField("founded", IntegerType(), True),
        StructField("employees", IntegerType(), True),
        StructField("vehicles", IntegerType(), True),
        StructField("launch_sites", IntegerType(), True),
        StructField("test_sites", IntegerType(), True),
        StructField("ceo", StringType(), True),
        StructField("cto", StringType(), True),
        StructField("coo", StringType(), True),
        StructField("cto_propulsion", StringType(), True),
        StructField("valuation", LongType(), True),
        StructField("summary", StringType(), True),
        StructField("id", StringType(), True),
        StructField("hq_addr", StringType(), True),
        StructField("hq_city", StringType(), True),
        StructField("hq_state", StringType(), True),
        StructField("website", StringType(), True),
        StructField("flickr", StringType(), True),
        StructField("twitter", StringType(), True),
        StructField("elon_twitter", StringType(), True)
    ])
   company_dict = company.to_dict()
   file = File(spark, sc, result=[company_dict], schema=schema, s3loc=f"s3://anduril-takehome/{env}/company/", validators=dupe_check)
   return file



args = parser.parse_args()
env = args.env

# Process company data and write file to S3
company = process_company()
company_file = create_file(company, env)
company_df = company_file.dataframe
company_df.show()

# company_file.write_to_s3()

