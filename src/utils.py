
from datetime import datetime, timedelta
import re
import pytz
from pyspark.sql.types import StructType


def parse_iso_format(date_str):
    match = re.match(r'(.*?)([+-]\d{2}):(\d{2})$', date_str)
    if match:
        date_str, offset_hours, offset_minutes = match.groups()
        offset = timedelta(hours=int(offset_hours), minutes=int(offset_minutes))
    else:
        offset = timedelta(0)
    dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
    dt = dt - offset
    dt = dt.replace(tzinfo=pytz.utc)
    return dt


class File:
  def __init__(self, spark, sc, result: list, schema: StructType, s3loc: str, overwrite_partition_where: str = "", validators: list = []):
    self.spark = spark
    self.rdd = sc.parallelize(result)
    self.schema = schema
    self.dataframe = self.__create_dataframe(rdd=self.rdd, schema=self.schema)
    self.validators = validators
    self.s3loc = s3loc
    self.overwrite_partition_where = overwrite_partition_where

  def __create_dataframe(self, rdd, schema):
    spark_dataframe = self.spark.createDataFrame(self.rdd, schema=self.schema)
    return spark_dataframe
  
  def __validate_dataframe(self):
    dataframe = self.dataframe
    validators = self.validators
    quality_check = True
    for check in validators:
      result = check(dataframe)
      if not result:
         quality_check = False
    return quality_check

  def write_to_s3(self, aws_access_key_id: str, aws_secret_access_key: str, aws_session_token: str = None):
    quality_check = self.__validate_dataframe()
    if quality_check is True:
        self.dataframe.write.format("parquet").mode("overwrite").save(self.s3loc)
        return True
    else:
        return False

  def overwrite_partition(self):
    quality_check = self.__validate_dataframe()
    if quality_check is True and self.overwrite_partition_where!= "":
      self.dataframe.write.format("parquet").mode("overwrite").option("replaceWhere", self.overwrite_partition_where).save(self.s3loc)
      return True
    else:
      return False
