import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import CONFIG as Config


class LandPad:
    def __init__(self, data):
        self.images = data.get('images', {}).get('large', [])
        self.name = data.get('name', "")
        self.full_name = data.get('full_name', "")
        self.status = data.get('status', "")
        self.type = data.get('type', "")
        self.locality = data.get('locality', "")
        self.region = data.get('region', "")
        self.latitude = data.get('latitude', 0.0)
        self.longitude = data.get('longitude', 0.0)
        self.landing_attempts = data.get('landing_attempts', 0)
        self.landing_successes = data.get('landing_successes', 0)
        self.wikipedia = data.get('wikipedia', "")
        self.details = data.get('details', "")
        self.launches = data.get('launches', [])
        self.id = data.get('id', "")


def process_landpads():
  result = []
  http = requests.Session()
  http.mount("https://" , Config.adapter)
  http.mount("http://", Config.adapter)
  r = http.get("https://api.spacexdata.com/v4/landpads")
  if r.status_code == requests.codes.ok:
     landing_pads = r.json()
     for lp in landing_pads:
        landing_pad = LandPad(lp)
        result.append(landing_pad)
  else:
     r.raise_for_status()
  return result


landpads = process_landpads()

# unfinished