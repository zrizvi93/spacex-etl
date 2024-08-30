import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import CONFIG as Config


class Ship:
    def __init__(self, data):
        self.legacy_id = data.get('legacy_id', "")
        self.model = data.get('model', None)
        self.type = data.get('type', "")
        self.roles = data.get('roles', [])
        self.imo = data.get('imo', 0)
        self.mmsi = data.get('mmsi', 0)
        self.abs = data.get('abs', 0)
        self.ship_class = data.get('class', 0)
        self.mass_kg = data.get('mass_kg', 0)
        self.mass_lbs = data.get('mass_lbs', 0)
        self.year_built = data.get('year_built', 0)
        self.home_port = data.get('home_port', "")
        self.status = data.get('status', "")
        self.speed_kn = data.get('speed_kn', None)
        self.course_deg = data.get('course_deg', None)
        self.latitude = data.get('latitude', None)
        self.longitude = data.get('longitude', None)
        self.last_ais_update = data.get('last_ais_update', None)
        self.link = data.get('link', "")
        self.image = data.get('image', "")
        self.launches = data.get('launches', [])
        self.name = data.get('name', "")
        self.active = data.get('active', False)
        self.id = data.get('id', "")


def process_ships():
  result = []
  http = requests.Session()
  http.mount("https://" , Config.adapter)
  http.mount("http://", Config.adapter)
  r = http.get("https://api.spacexdata.com/v4/ships")
  if r.status_code == requests.codes.ok:
     ships = r.json()
     for s in ships:
        ship = Ship(s)
        result.append(ship)
  else:
     r.raise_for_status()
  return result


ships = process_ships()

# unfinished