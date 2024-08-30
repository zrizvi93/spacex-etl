import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

from config import CONFIG as Config
from utils import File

spark = SparkSession.builder.appName("CompanyDataExtraction").getOrCreate()
sc = spark.sparkContext

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", Config.aws_access_key)
hadoop_conf.set("fs.s3a.secret.key", Config.aws_secret_key)
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

class Company:
   def __init__(self, data):
      self.headquarters = data.get('headquarters', {})
      self.links = data.get('links', {})
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
         "headquarters": self.headquarters,
         "links": self.links,
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
     # do some stuff 
     company_data = r.json()
     company = Company(company_data)
  else:
     r.raise_for_status()
  return company

def create_file(company):
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
        StructField("headquarters", StringType(), True),
        StructField("links", StringType(), True)
    ])

   company_dict = company.to_dict()
   file = File(spark, sc, result=[company_dict], schema=schema, s3loc="s3://anduril-takehome/company/")
   return file

# Process company data and write file to S3
company = process_company()
company_file = create_file(company)
company_df = company_file.dataframe
company_df.show()

# company_file.write_to_s3()

