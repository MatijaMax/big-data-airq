import requests
import os
from dotenv import load_dotenv

# curl --location -g 'http://api.airvisual.com/v2/city?city=Los%20Angeles&state=California&country=USA&key={{YOUR_API_KEY}}'
# curl --location -g 'http://api.airvisual.com/v2/states?country=USA&key={{YOUR_API_KEY}}'
# curl --location -g 'http://api.airvisual.com/v2/cities?state=New%20York&country=USA&key={{YOUR_API_KEY}}'



def test_api():

  load_dotenv()
  API_KEY = os.getenv('API_KEY')
  URL = f"http://api.airvisual.com/v2/city?city=Los%20Angeles&state=California&country=USA&key={API_KEY}"
  response = requests.get(URL)
  print(response.json())

if __name__ == '__main__':
  test_api()

# ENTITIES
# HISTORIC DATA
# ~
# REAL TIME DATA WITH SIMULATED STREAM
# ~