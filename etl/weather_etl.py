import pandas as pd
import openmeteo_requests
import logging
import os
import requests_cache
from retry_requests import retry

# Load environment variables safely (provide defaults for local testing)
URL = os.getenv("OPENMETEO_API_URL", "https://archive-api.open-meteo.com/v1/archive")
PATH = os.getenv("REGIONS_BOUNDARIES_PATH", "data/regions_boundries.json")
START_DATE = os.getenv("WEATHER_START_DATE", "2022-01-01")
END_DATE = os.getenv("WEATHER_END_DATE", "2025-07-31")
CACHE_DIR = os.getenv("REQUESTS_CACHE_DIR", ".cache")
LOG_PATH = os.getenv("WEATHER_LOG_PATH", "logs/weather_etl.log")
DATA_DIR = os.getenv("DATA_DIR", "data")

# Setup logging
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Setup cache and retry for requests
cache_session = requests_cache.CachedSession(CACHE_DIR, expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def fetch_weather_data(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": [
            "temperature_2m_mean", 
            "cloud_cover_mean", 
            "wind_speed_10m_max", 
            "rain_sum", 
            "snowfall_sum"
        ]   
    }

    responses = openmeteo.weather_api(URL, params=params)
    for response in responses:
        print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation: {response.Elevation()} m asl")
        print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

        daily = response.Daily()
        daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
        daily_cloud_cover_mean = daily.Variables(1).ValuesAsNumpy()
        daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
        daily_rain_sum = daily.Variables(3).ValuesAsNumpy()
        daily_snowfall_sum = daily.Variables(4).ValuesAsNumpy()

        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            )
        }

        daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
        daily_data["cloud_cover_mean"] = daily_cloud_cover_mean
        daily_data["rain_sum"] = daily_rain_sum
        daily_data["snowfall_sum"] = daily_snowfall_sum
        daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max

        daily_dataframe = pd.DataFrame(data=daily_data)
    return daily_dataframe

def extract(lat, lon):
    return fetch_weather_data(str(lat), str(lon))

def transform(path):
    df = pd.read_json(path).T

    for region in df.index:
        south = df.loc[region, 'min_lat']
        north = df.loc[region, 'max_lat']
        west = df.loc[region, 'min_lon']
        east = df.loc[region, 'max_lon']
        lat = (south + north) / 2
        lon = (west + east) / 2
        if region.lower() != "unknown":
            logging.info(f"Extracting weather data for region: {region}")
            df_tmp = extract(lat, lon)
            logging.info(f"Successfully extracted weather data for region: {region} from {START_DATE} to {END_DATE}")
            df_tmp.to_csv(f"{DATA_DIR}/weather_data_{region}.csv", index=False)

def weather_etl(path=PATH):
    logging.info("Starting weather ETL process")
    transform(path)
    logging.info("Weather ETL process completed successfully")

if __name__ == "__main__":
    weather_etl()
