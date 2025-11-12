import pandas as pd

def merge_weather_tourism(tourism_path):
    tourism=pd.read_csv(tourism_path)
    regions=tourism['Region'].unique()
    final_df=pd.DataFrame()
    for region in regions:
        if region.lower()!="provincia":
            weather_data=pd.read_csv(f"data/weather_data_{region}.csv")
            weather_data["date"]=pd.to_datetime(weather_data["date"])
            weather_data['rainy_day']=weather_data['rain_sum']>0
            weather_data['snowy_day']=weather_data['snowfall_sum']>0
            weather_data["year"]=weather_data["date"].dt.year
            weather_data['month']=weather_data["date"].dt.month
            rainy=weather_data.groupby(['year','month'])[['rainy_day','snowy_day']].sum().reset_index()
            reste=weather_data.groupby(['year','month']).mean().reset_index().iloc[:,:-2]
            monthly_df=reste.merge(rainy,on=['year','month'])
            merged=tourism[tourism['Region']==region].merge(monthly_df,left_on=["Year",'Month_Num'],right_on=['year','month']).drop(columns=['year','month','date'])
            final_df=pd.concat([final_df,merged],ignore_index=True)
            final_df["Total_presence"]=final_df["Italians"]+final_df["Foreigners"]
            

    return final_df

def get_season(month):
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "autumn"

def compute_weather_score(row,scaling_params,n_days=30):
    t = row["temperature_2m_mean"]
    rainy_days = row["rainy_day"]
    snow = row["snowfall_sum"]
    snowy_days = row["snowy_day"]
    cloud = row["cloud_cover_mean"]
    wind = row["wind_speed_10m_max"]
    max_snow = (scaling_params["max_snowfall_sum"] / 30) * n_days
    max_wind = scaling_params["max_wind_speed"]
    
    if row["season"] == "summer":
        score = (
            0.5 * (1 - abs(t - 25) / 25) +  
            0.3 * (1 - rainy_days / n_days) +
            0.2 * (1 - cloud / 100)
        )
    elif row["season"] == "winter":
        score = (
            0.45 * (1 - abs(t + 2) / 15) +  
            0.4 * (snow / max_snow) +
            0.15 * (1 - wind / max_wind)+
            0.05 * (snowy_days / n_days) 
        )
    else:  
        score = (
            0.5 * (1 - abs(t - 18) / 18) +
            0.3 * (1 - rainy_days / n_days) +
            0.2 * (1 - cloud / 100)
        )
        
    return max(0, min(1, score))
