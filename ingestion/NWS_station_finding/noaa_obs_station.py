import requests
import json
from tqdm import tqdm


def get_noaa_obs_station(lat, lon):
    """Get the NOAA observation station for a given latitude and longitude."""
    url = f"https://api.weather.gov/points/{lat},{lon}/stations"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def main():
    """Main function to get the NOAA observation station for a given latitude and longitude."""
    path = "state_coordinates.txt"
    with open(path, "r") as f:
        state_to_coords = json.load(f)
    states_listed = []
    weather_stations = {}
    station_ids = []
    for state, coords in tqdm(state_to_coords.items()):
        if state not in states_listed:
            states_listed.append(state)
            weather_stations[state] = []
            print(state)
        for coord in tqdm(coords):
            lon, lat = coord
            response = get_noaa_obs_station(lat, lon)
            api_link = str(response["features"][0]["id"])
            station_id = api_link.rstrip("/").rsplit("/", 1)[-1]
            if station_id not in station_ids:
                station_ids.append(station_id)
                weather_stations[state].append(station_id)

    # Write the dictionary of lists to a txt file as JSON
    output_path = "weather_stations.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(weather_stations, f, indent=2)


if __name__ == "__main__":
    main()
