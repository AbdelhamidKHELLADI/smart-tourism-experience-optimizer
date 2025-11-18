import pandas as pd
from utils.preprocess_utils import merge_weather_tourism,compute_weather_score,get_season,categorize_experience
from utils.s3_utils import save_to_s3,save_json_to_s3
import os
BUCKET_NAME=os.getenv("TOURISM_BUCKET")
if not BUCKET_NAME:
    raise RuntimeError("TOURISM_BUCKET env var not set (BUCKET_NAME is required)")

import logging
logging.basicConfig(   
    filename='logs/preprocess.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

TOURISM_PATH = os.getenv("TOURISM_DATA_PATH", f"tourism_movement_with_gtfs.csv")


def preprocess():
    df=merge_weather_tourism(TOURISM_PATH,BUCKET_NAME)

    df["mobility_index"] = (
        df.groupby("Region")["num_trips"]
        .transform(lambda x: (x - x.min()) / (x.max() - x.min()))
    )
    df["mobility_index"] = df["mobility_index"].fillna(0)
    df_tmp=df[["Region","Month_Num","mobility_index"]].drop_duplicates()
    save_to_s3(df_tmp,BUCKET_NAME,"mobility_index_per_region.csv")
    
    logging.info("mobility index per region saved sucessfuly")

    MAX_SNOWFALL = df["snowfall_sum"].max()
    MAX_WIND = df["wind_speed_10m_max"].max()
    scaling_params = {
    "max_snowfall_sum": float(MAX_SNOWFALL),
    "max_wind_speed": float(MAX_WIND)
    }


    save_json_to_s3(scaling_params,BUCKET_NAME,"scaling_params.json")

    df["season"] = df["Month_Num"].apply(get_season)

    df["weather_score"] = df.apply(lambda row: compute_weather_score(row,scaling_params), axis=1)

    df["presence_index"] = (
    df.groupby("Region")["Total_presence"]
    .transform(lambda x: (x - x.min()) / (x.max() - x.min()))
    )

    df['tourism_index']=(df["presence_index"]*0.4+df["weather_score"]*0.3+df['mobility_index']*0.3)
    df["year_month"]=df["Year"].astype(str)+'-'+df['Month_Num'].astype(str)
    df.set_index("year_month",inplace=True)
    df["experience_level"] = df["tourism_index"].apply(categorize_experience)
    df["Region"] = df["Region"].str.replace(",", "", regex=False)
    df["Region"] = df["Region"].str.replace(" ", "_", regex=False)

    df=pd.get_dummies(df,columns=["Region"],prefix='region_')
    save_to_s3(df,BUCKET_NAME,'preprocessed.csv')
    logging.info("Created preprocessed.csv for training ")

if __name__ == "__main__":
    preprocess()