import logging
import os
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from utils.preprocess_utils import compute_weather_score, get_season,categorize_experience
from utils.s3_utils import read_from_s3,save_to_s3,read_json_from_s3
import boto3
import xgboost as xgb


logging.basicConfig(

    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

PATH = os.getenv("REGIONS_BOUNDARIES_PATH", "regions_boundries.json")
EXISTING_PREDS_PATH = os.getenv("EXISTING_PREDS_PATH", "predictions.csv")
PREPROCESSED_PATH = os.getenv("PREPROCESSED_PATH", "mobility_index_per_region.csv")
SCALING_PARAMS_PATH = os.getenv("SCALING_PARAMS_PATH", "scaling_params.json")
WEEKLY_DATA_PATH = os.getenv("WEEKLY_DATA_PATH", "weekly_data.csv")

MLFLOW_MODEL_URI = os.getenv("MLFLOW_MODEL_URI", "models:/TourismPresenceXGB/18")
BUCKET_NAME=os.getenv("TOURISM_BUCKET")
 
if not BUCKET_NAME:
    raise RuntimeError("TOURISM_BUCKET env var not set (BUCKET_NAME is required)")

scaling_params = read_json_from_s3(BUCKET_NAME,SCALING_PARAMS_PATH)

cache_session = requests_cache.CachedSession('/tmp/requests_cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

url = "https://api.open-meteo.com/v1/forecast"

def forecast_weather(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": [
            "temperature_2m_mean", "cloud_cover_mean",
            "wind_speed_10m_max", "rain_sum", "snowfall_sum"
        ],
        "forecast_days": 15,
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

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
        ),
        "temperature_2m_mean": daily_temperature_2m_mean,
        "cloud_cover_mean": daily_cloud_cover_mean,
        "wind_speed_10m_max": daily_wind_speed_10m_max,
        "rain_sum": daily_rain_sum,
        "snowfall_sum": daily_snowfall_sum
    }

    return pd.DataFrame(data=daily_data)






def transform(path=PATH):
    df = pd.DataFrame(read_json_from_s3(BUCKET_NAME, path)).T
    all_regions_df = []

    for region in df.index:
        if region.lower() != "unknown":
            south = df.loc[region, 'min_lat']
            north = df.loc[region, 'max_lat']
            west = df.loc[region, 'min_lon']
            east = df.loc[region, 'max_lon']
            lat = (south + north) / 2
            lon = (west + east) / 2

            logging.info(f"Forecasting weather data for region: {region}")
            df_tmp = forecast_weather(lat, lon)
            df_tmp["rainy_day"] = df_tmp["rain_sum"] > 0
            df_tmp["snowy_day"] = df_tmp["snowfall_sum"] > 0
            df_tmp["Region"] = region
            df_tmp["week"] = df_tmp["date"].dt.isocalendar().week
            df_tmp["month"] = df_tmp.date.dt.month
            df_tmp["year"] = df_tmp["date"].dt.year

            weekly = (
                df_tmp.groupby(["Region", "year", "week"])
                .agg({
                    "month": "max",
                    "temperature_2m_mean": "mean",
                    "cloud_cover_mean": "mean",
                    "wind_speed_10m_max": "mean",
                    "rain_sum": "sum",
                    "snowfall_sum": "sum",
                    "rainy_day": "sum",
                    "snowy_day": "sum"
                })
                .reset_index()
            )
            all_regions_df.append(weekly)

    return pd.concat(all_regions_df, ignore_index=True)


def preprocess(path=PATH):
    df = transform(path)
    preprocessed = read_from_s3(BUCKET_NAME,PREPROCESSED_PATH)

    df["season"] = df["month"].apply(get_season)
    df["weather_score"] = df.apply(lambda row: compute_weather_score(row, scaling_params), axis=1)
    df = df.merge(preprocessed, left_on=["Region", "month"], right_on=["Region", "Month_Num"])
    df["year_week"] = df.year.astype(str) + "-" + df.week.astype(str)
    df.set_index("year_week", inplace=True)

    df["Region"] = df["Region"].str.replace(",", "", regex=False)
    df["Region"] = df["Region"].str.replace(" ", "_", regex=False)

    df = pd.get_dummies(df, columns=["Region"], prefix="region_")
    
    save_to_s3(df, bucket_name=BUCKET_NAME, key=WEEKLY_DATA_PATH)

    logging.info(f"Created {WEEKLY_DATA_PATH} for predictions")
    return df


def predict(df):
    s3 = boto3.client("s3")
    key = "models/xgb.pkl"
    local_path = "/tmp/xgb.pkl" 
    s3.download_file(BUCKET_NAME, key, local_path)
    model = xgb.Booster()
    model.load_model(local_path)

    region_cols=[col for col in df.columns if col.startswith("region_")]
    X = df[[
        "Month_Num", "mobility_index", "weather_score",
        "temperature_2m_mean", "cloud_cover_mean",
        "snowfall_sum", "snowy_day"
    ] + region_cols]

    
    dmatrix = xgb.DMatrix(X)
    preds = model.predict(dmatrix)
    return preds
def save_new_preds(df,preds):
    df["tourism_index"] = preds
    df["experience_level"] = df["tourism_index"].apply(categorize_experience)
    region_cols=[col for col in df.columns if col.startswith("region_")]
    df["Region"]=df[region_cols].idxmax(axis=1).str.replace("region__","")
    df["Region"]=df["Region"].str.replace('_', " ")
    df.columns = df.columns.str.strip()
    try:
        existing_pred = read_from_s3(BUCKET_NAME,EXISTING_PREDS_PATH)
        if not existing_pred.empty:
            logging.info(f"Found prediction until week {existing_pred.week.max()} of {existing_pred.year.max()}")
            if existing_pred.year.max() <= df.year.max():
                if existing_pred.week.max() < df.week.max():
                    new_df = pd.concat([existing_pred, df], ignore_index=True).drop_duplicates(subset=["year", "week", "Region"], keep="first")
                    save_to_s3(new_df,BUCKET_NAME,EXISTING_PREDS_PATH)
                    logging.info(f"Predictions added from week {existing_pred.week.max()} to {df.week.max()} of {df.year.max()}")

                else:
                    logging.info("prediction are up to date")
            
                    
    except Exception:
        logging.info("No predictions found. Creating new predictions file.")
        logging.info(f"Predictions added from week {df.week.min()} to {df.week.max()} of {df.year.max()}")
        save_to_s3(df,BUCKET_NAME,EXISTING_PREDS_PATH)

    
def main():
    df=preprocess(PATH)
    preds=predict(df)
    save_new_preds(df,preds)


if __name__ == "__main__":
    main()
