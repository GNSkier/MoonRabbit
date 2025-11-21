import pandas as pd
import requests
import json
from datetime import datetime
from meteostat import Point, Hourly
from tqdm import tqdm

coco_mapping = {
        1: "Clear",
        2: "Fair (mostly clear)",
        3: "Partly Cloudy",
        4: "Overcast",
        5: "Fog",
        7: "Light Rain",
        8: "Rain",
        9: "Heavy Rain",
        14: "Light Snowfall",
        15: "Moderate Snowfall",
        23: "Lightning",
        24: "Hail",
        25: "Thunderstorm",
        27: "Storm Conditions",
    }

def get_noaa_obs_station(lat, lon):
    """Get the NOAA observation station for a given latitude and longitude."""
    url = f"https://api.weather.gov/points/{lat},{lon}/stations"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def pull_historical_weather(path:str):
    """Function to pull historical weather data using meteostat

    Args:
        path (str): pathway to text file housing longitude and latitude coordinates.
    Returns: 
        output (list): list of dataframes for weather data. 
    """
    with open(path, "r") as f:
        state_to_coords = json.load(f)
    output = []
    for state, coords in state_to_coords.items():
        print(state)
        for coord in tqdm(coords):
            lon, lat = coord
            start = datetime(2020, 1, 1)
            end = datetime(2025, 11, 1)
            location = Point(lat, lon)
            data = Hourly(location, start=start, end=end).fetch()
            data = data.reset_index()
            data["lon"] = lon
            data["lat"] = lat
            output.append(data)
    return output

def find_noaa_stations(weather_df:pd.DataFrame):

    weather_df_stations = weather_df.copy()
    lat_lons = []
    for lat, lon in zip(weather_df_stations["lat"], weather_df_stations["lon"]):
        if (lat, lon) not in lat_lons:
            lat_lons.append((lat, lon))
            response = get_noaa_obs_station(lat, lon)
            api_link = str(response["features"][0]["id"])
            station_id = api_link.rstrip("/").rsplit("/", 1)[-1]
            weather_df_stations.loc[
                (weather_df_stations["lat"] == lat)
                & (weather_df_stations["lon"] == lon),
                "station_id",
            ] = station_id
    return weather_df_stations


if __name__ == "__main__":
    path = "../NWS_station_finding/state_coordinates.txt"
    weather_list = pull_historical_weather(path)

    stack_df = pd.concat(weather_list, axis=0, ignore_index=True)

    combined_weather = pd.concat(weather_list, axis=0, ignore_index=True)
    combined_weather["weather_condition"] = combined_weather["coco"].map(
        coco_mapping
    )
    historical_weather_df = find_noaa_stations(combined_weather)
    historical_weather_df = historical_weather_df[
        [
            "time",
            "station_id",
            "lat",
            "lon",
            "temp",
            "dwpt",
            "rhum",
            "prcp",
            "wdir",
            "wspd",
            "wpgt",
            "pres",
            "weather_condition",
        ]
    ]
    historical_weather_df.to_parquet(
        "data/historical_weather_df.parquet",
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
        index=False,
    )
