# AirVisual API producer
import os
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
import json

def wait_for_kafka(bs='kafka:9092', timeout=60):
    start = time.time()
    while True:
        try:
            KafkaProducer(bootstrap_servers=bs)
            print("✅ Kafka is ready")
            return
        except NoBrokersAvailable:
            print("⏳ Waiting for Kafka...")
            time.sleep(3)

def fetch_air_quality_data(location):
    load_dotenv()
    API_KEY = os.getenv("API_KEY")
    URL = (
        f"http://api.airvisual.com/v2/city?"
        f"city={location['city']}&state={location['state']}&country={location['country']}&key={API_KEY}"
    )
    response = requests.get(URL)
    return response.json()

def main():
    print("START PRODUCER")
    locations = [
        # CALIFORNIA
        {"city": "Los Angeles", "state": "California", "country": "USA"},
        {"city": "San Francisco", "state": "California", "country": "USA"},
        {"city": "San Diego", "state": "California", "country": "USA"},
        {"city": "Sacramento", "state": "California", "country": "USA"},
        {"city": "San Jose", "state": "California", "country": "USA"},
        {"city": "Oakland", "state": "California", "country": "USA"},
        {"city": "Beverly Hills", "state": "California", "country": "USA"},
        {"city": "Pasadena", "state": "California", "country": "USA"},
        {"city": "Santa Monica", "state": "California", "country": "USA"},
        {"city": "Long Beach", "state": "California", "country": "USA"},
        # TEXAS
        {"city": "Austin", "state": "Texas", "country": "USA"},
        {"city": "Dallas", "state": "Texas", "country": "USA"},
        {"city": "Houston", "state": "Texas", "country": "USA"},
        {"city": "San Antonio", "state": "Texas", "country": "USA"},
        {"city": "Fort Worth", "state": "Texas", "country": "USA"},
        {"city": "El Paso", "state": "Texas", "country": "USA"},
        {"city": "Arlington", "state": "Texas", "country": "USA"},
        {"city": "Corpus Christi", "state": "Texas", "country": "USA"},
        {"city": "Plano", "state": "Texas", "country": "USA"},
        {"city": "Laredo", "state": "Texas", "country": "USA"},
        # ILLINOIS
        {"city": "Chicago", "state": "Illinois", "country": "USA"},
        {"city": "Aurora", "state": "Illinois", "country": "USA"},
        {"city": "Naperville", "state": "Illinois", "country": "USA"},
        {"city": "Joliet", "state": "Illinois", "country": "USA"},
        {"city": "Rockford", "state": "Illinois", "country": "USA"},
        {"city": "Peoria", "state": "Illinois", "country": "USA"},
        {"city": "Springfield", "state": "Illinois", "country": "USA"},
        {"city": "Champaign", "state": "Illinois", "country": "USA"},
        {"city": "Elgin", "state": "Illinois", "country": "USA"},
        {"city": "Waukegan", "state": "Illinois", "country": "USA"},
        # PENNSYLVANIA
        {"city": "Philadelphia", "state": "Pennsylvania", "country": "USA"},
        {"city": "Pittsburgh", "state": "Pennsylvania", "country": "USA"},
        {"city": "Allentown", "state": "Pennsylvania", "country": "USA"},
        {"city": "Erie", "state": "Pennsylvania", "country": "USA"},
        {"city": "Reading", "state": "Pennsylvania", "country": "USA"},
        {"city": "Scranton", "state": "Pennsylvania", "country": "USA"},
        {"city": "Bethlehem", "state": "Pennsylvania", "country": "USA"},
        {"city": "Lancaster", "state": "Pennsylvania", "country": "USA"},
        {"city": "Harrisburg", "state": "Pennsylvania", "country": "USA"},
        {"city": "York", "state": "Pennsylvania", "country": "USA"},
        # UTAH
        {"city": "Salt Lake City", "state": "Utah", "country": "USA"},
        {"city": "Provo", "state": "Utah", "country": "USA"},
        {"city": "Ogden", "state": "Utah", "country": "USA"},
        {"city": "Sandy", "state": "Utah", "country": "USA"},
        {"city": "West Jordan", "state": "Utah", "country": "USA"},
        {"city": "Layton", "state": "Utah", "country": "USA"},
        {"city": "Orem", "state": "Utah", "country": "USA"},
        {"city": "Bountiful", "state": "Utah", "country": "USA"},
        {"city": "Draper", "state": "Utah", "country": "USA"},
        {"city": "Farmington", "state": "Utah", "country": "USA"}       
    ]

    wait_for_kafka("kafka:9092")

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    while True:
        for location in locations:
            data = fetch_air_quality_data(location)
            producer.send("airq", value=data)
            print(f"Sent data for {location['city']}: {data}")
            #time.sleep(10)  # Fetch next city data after 10 seconds
            time.sleep(15)  # Fetch next city data after 15 seconds
            #time.sleep(1)  # Fetch next city data after 1 second
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Script crashed with exception: {e}")
    finally:
        print("⚠️ Producer script has exited")
