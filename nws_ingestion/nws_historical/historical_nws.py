import pandas as pd
import requests
import json
from datetime import datetime
from meteostat import Point, Hourly
import logging
from requests.exceptions import RequestException


def get_noaa_obs_station(lat, lon):
    """Get the NOAA observation station for a given latitude and longitude."""
    url = f"https://api.weather.gov/points/{lat},{lon}/stations"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.warning(
            "Station lookup request failed for (%s, %s): %s", lat, lon, e
        )
        return None
    except ValueError as e:
        logging.warning(
            "Station lookup returned invalid JSON for (%s, %s): %s", lat, lon, e
        )
        return None


def get_historical_nws(start_date: datetime, end_date: datetime):
    path = "../NWS_station_finding/state_coordinates.txt"
    try:
        with open(path, "r") as f:
            state_to_coords = json.load(f)
    except FileNotFoundError:
        logging.error("Coordinates file not found at path: %s", path)
        return pd.DataFrame()
    except json.JSONDecodeError as e:
        logging.error("Coordinates file is not valid JSON: %s", e)
        return pd.DataFrame()

    dfs = []
    for state, coords in state_to_coords.items():
        logging.info("Processing state: %s", state)
        for coord in coords:
            lon, lat = coord
            try:
                location = Point(lat, lon)
                data = Hourly(location, start=start_date, end=end_date).fetch()
                data = data.reset_index()
            except Exception as e:
                logging.warning(
                    "Failed Meteostat fetch for lat=%s lon=%s: %s", lat, lon, e
                )
                continue

            station_id = None
            try:
                response = get_noaa_obs_station(lat, lon)
                if response and response.get("features"):
                    api_link = str(response["features"][0]["id"])
                    station_id = api_link.rstrip("/").rsplit("/", 1)[-1]
                else:
                    logging.warning(
                        "No station features found for lat=%s lon=%s", lat, lon
                    )
            except Exception as e:
                logging.warning(
                    "Station id extraction failed for lat=%s lon=%s: %s",
                    lat,
                    lon,
                    e,
                )

            data["lon"] = lon
            data["lat"] = lat
            data["station_id"] = station_id
            dfs.append(data)

    if not dfs:
        logging.warning(
            "No data collected from any coordinates; returning empty DataFrame."
        )
        return pd.DataFrame()

    stack_df = pd.concat(dfs, axis=0, ignore_index=True)
    historical_weather_df = stack_df.drop(columns=["index"], errors="ignore")
    return historical_weather_df


def map_weather_code_coco(weather_df: pd.DataFrame) -> pd.DataFrame:
    """Map the weather code to the corresponding weather condition."""
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

    try:
        if "coco" not in weather_df.columns:
            logging.warning(
                "Column 'coco' not found; adding empty 'weather_condition'."
            )
            weather_df["weather_condition"] = None
            return weather_df
        weather_df["weather_condition"] = weather_df["coco"].map(coco_mapping)
        return weather_df.drop(columns=["coco"])
    except Exception as e:
        logging.warning("Failed to map 'coco' to weather_condition: %s", e)
        return weather_df


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 11, 1)
    try:
        historical_weather_df = get_historical_nws(start_date, end_date)
    except Exception as e:
        logging.error("Uncaught error during historical fetch: %s", e)
        historical_weather_df = pd.DataFrame()

    if historical_weather_df.empty:
        logging.warning(
            "Historical DataFrame is empty; skipping mapping and export."
        )
    else:
        try:
            historical_weather_df_coded = map_weather_code_coco(
                historical_weather_df
            )
        except Exception as e:
            logging.error("Failed to map weather codes: %s", e)
            historical_weather_df_coded = historical_weather_df

        try:
            historical_weather_df_coded = historical_weather_df_coded[
                [
                    "time",
                    "station_id",
                    "lat",
                    "lon",
                    "temp",
                    "dwpt",
                    "rhum",
                    "prcp",
                    "snow",
                    "wdir",
                    "wspd",
                    "wpgt",
                    "pres",
                    "tsun",
                    "weather_condition",
                ]
            ]
        except KeyError as e:
            logging.warning(
                "Some expected columns missing during selection: %s", e
            )

        try:
            historical_weather_df_coded.to_json(
                "data/historical_weather_df_coded.json", index=False
            )
        except OSError as e:
            logging.error("Failed to write output JSON: %s", e)
