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
##### HISTORIC DATA #####

# This dataset deals with pollution in the U.S. Pollution in the U.S. has been well documented by the U.S. EPA.

# Includes four major pollutants (Nitrogen Dioxide, Sulphur Dioxide, Carbon Monoxide and Ozone).

# BatchEntity:

#     State Code : The code allocated by US EPA to each state
#     County code : The code of counties in a specific state allocated by US EPA
#     Site Num : The site number in a specific county allocated by US EPA
#     Address: Address of the monitoring site
#     State : State of monitoring site
#     County : County of monitoring site
#     City : City of the monitoring site
#     Date Local : Date of monitoring

# The four pollutants (NO2, O3, SO2 and O3) each has 5 specific columns. For instance, for NO2:

#     NO2 Units : The units measured for NO2
#     NO2 Mean : The arithmetic mean of concentration of NO2 within a given day
#     NO2 AQI : The calculated air quality index of NO2 within a given day
#     NO2 1st Max Value : The maximum value obtained for NO2 concentration in a given day
#     NO2 1st Max Hour : The hour when the maximum NO2 concentration was recorded in a given day

##### REAL TIME DATA WITH SIMULATED STREAM #####

# StreamEntity:

#     city (String) – Name of the city (e.g., "Los Angeles").
#     state (String) – Name of the state (e.g., "California").
#     country (String) – Name of the country (e.g., "USA").
#     location (Object) – Geographical details.
#         type (String) – Type of location data (e.g., "Point").
#         coordinates (Array of Float) – Longitude and latitude values (e.g., [-118.2417, 34.0669]).
#     current (Object) – Current environmental conditions.
#         pollution (Object) – Air quality data.
#             ts (String, Timestamp) – Timestamp of the data (e.g., "2025-03-05T14:00:00.000Z").
#             aqius (Integer) – Air Quality Index (AQI) for the USA (e.g., 48).
#             mainus (String) – Main pollutant in the USA (e.g., "p2").
#             aqicn (Integer) – AQI for China (e.g., 25).
#             maincn (String) – Main pollutant in China (e.g., "p1").
#         weather (Object) – Weather conditions.
#             ts (String, Timestamp) – Timestamp of the data.
#             tp (Integer, °C) – Temperature in Celsius (e.g., 10).
#             pr (Integer, hPa) – Atmospheric pressure (e.g., 1016).
#             hu (Integer, %) – Humidity percentage (e.g., 91).
#             ws (Float, m/s) – Wind speed in meters per second (e.g., 0).
#             wd (Integer, °) – Wind direction in degrees (e.g., 0).
#             ic (String) – Weather icon code (e.g., "04n").
