import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import datetime
from datetime import timedelta, date

import os

class Config:
   batch_id = int(datetime.datetime.utcnow().timestamp())
   retry_strategy = Retry(
      total=10,
      backoff_factor=1,
      status_forcelist=[429, 500, 502, 503, 504],
      allowed_methods=["GET"])
   adapter = HTTPAdapter(max_retries=retry_strategy)
   api_base_url = "https://api.spacexdata.com"
   api_version = "v4"

   aws_access_key = os.environ.get('AWS_ACCESS_KEY')
   aws_secret_key = os.environ.get('AWS_SECRET_KEY')

CONFIG = Config()