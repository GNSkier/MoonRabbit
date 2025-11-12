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

def extract_features(data:json):
    """Function to extract key features from the NWS API Json pull.

    Args:
        data (json): The raw NWS API JSON from call.
    Returns:
        output_features (dict): A dictionary of key features.
    """
    props = data.get("properties") or {}
    geom = data.get("geometry") or {}
    coords = geom.get("coordinates")

    lon = (
        coords[0] if isinstance(coords, (list, tuple)) and len(coords) > 0 else None
    )
    lat = (
        coords[1] if isinstance(coords, (list, tuple)) and len(coords) > 1 else None
    )

    output_features = {
        "timestamp": props.get("timestamp"),
        "stationId": props.get("stationId"),
        "stationName": props.get("stationName"),
        "lat": lat,
        "lon": lon,
        "elevation": (props.get("elevation") or {}).get("value"),
        "temp_unit": (props.get("temperature") or {}).get("unitCode"),
        "temp": (props.get("temperature") or {}).get("value"),
        "pressure_pa": (props.get("barometricPressure") or {}).get("value"),
        "humidity": (props.get("relativeHumidity") or {}).get("value"),
        "wind_unit": (props.get("windSpeed") or {}).get("unitCode"),
        "wind_speed": (props.get("windSpeed") or {}).get("value"),
        "precip_unit": (props.get("precipitationLast3Hours") or {}).get("unitCode"),
        "precip_3hr": (props.get("precipitationLast3Hours") or {}).get("value"),
        "heat_unit": (props.get("heatIndex") or {}).get("unitCode"),
        "heat_index": (props.get("heatIndex") or {}).get("value"),
        "max_temp_24_unit": (props.get("maxTemperatureLast24Hours") or {}).get(
            "unitCode"
        ),
        "max_temp_24": (props.get("maxTemperatureLast24Hours") or {}).get("value"),
        "min_temp_24_unit": (props.get("minTemperatureLast24Hours") or {}).get(
            "unitCode"
        ),
        "min_temp_24": (props.get("minTemperatureLast24Hours") or {}).get("value"),
    }
    return output_features

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

    key_features = extract_features(data)

    message_bytes = json.dumps(key_features).encode("utf-8")

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
