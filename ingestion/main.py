import os
import json
import requests
import time
from google.cloud import pubsub_v1
from utilities import weather_stations

PROJECT_ID = os.getenv("GCP_PROJECT")
TOPIC_ID = "nws_api_soy"
API_URL = "https://api.weather.gov/stations/{station_id}/observations/latest"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def fetch_and_publish(station_id: str):
    """Function to fetch and publish recent NWS station data.

    Args:
        station_id (str): A string of the NWS weather station id.
    """
    api_url = (
        f"https://api.weather.gov/stations/{station_id}/observations/latest"
    )

    print(f"Fetching data from {api_url}...")
    headers = {"User-Agent": f"MoonRabbit/1.0 ({os.getenv('USER_EMAIL')})"}
    request = requests.get(api_url, headers=headers)
    request.raise_for_status()
    data = request.json()

    message_bytes = json.dumps(data).encode("utf-8")

    future = publisher.publish(topic_path, message_bytes)
    print(f"Published Message ID: {future.result()}")


def extract_station_id(weather_stations: dict):
    """Extracts all Weather Station IDs"""
    for states, stations in weather_stations.items():
        for station_id in stations:
            yield station_id


if __name__ == "__main__":
    if not PROJECT_ID:
        print("WARNING: GCP_PROJECT has not been set:")

    for station_id in extract_station_id(weather_stations):
        try:
            fetch_and_publish(station_id)
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {station_id}: {e}")
        except Exception as e:
            print(f"Error {station_id},{e}")
        time.sleep(0.2)
