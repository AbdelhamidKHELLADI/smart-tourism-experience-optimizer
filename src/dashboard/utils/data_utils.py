from utils.s3_utils import  read_from_s3
import streamlit as st
import pandas as pd

EXPERIENCE_STYLES = {
    "Not Ideal": {"bg": "#f2f6fa", "emoji": "😴"},
    "Quiet Season": {"bg": "#f9f9f9", "emoji": "🛌"},
    "Moderate Season": {"bg": "#fff9e6", "emoji": "🙂"},
    "Popular Season": {"bg": "#e8f5e9", "emoji": "😎"},
    "Peak Season": {"bg": "#ffe6e6", "emoji": "🔥"},
}

@st.cache_data
def load_data_cached(bucket_name,data_path):
    df = read_from_s3(bucket_name, data_path)
    return df[["year", "week", "Region", "tourism_index", "experience_level"]]

@st.cache_data
def load_forecast(bucket_name,data_path):
    df=read_from_s3(bucket_name,data_path)
    region_cols = [col for col in df.columns if col.startswith("region_")]
    df["Region"] = df[region_cols].idxmax(axis=1).str.replace("region__", "")
    df["Region"] = df["Region"].str.replace('_', " ")
    return df[["Year", "Month_Num", "Region", "season", "tourism_index", "experience_level"]]

def categorize_experience(score):
    if score < 0.1: return "Not Ideal"
    elif score < 0.4: return "Quiet Season"
    elif score < 0.6: return "Moderate Season"
    elif score < 0.75: return "Popular Season"
    else: return "Peak Season"

def get_forecast_weeks(current_week, current_year, week_choice):
    next_week = current_week + 1
    next_week_year = current_year
    if next_week > 52:
        next_week = 1
        next_week_year += 1
    if week_choice == "This week":
        return [(current_year, current_week)]
    elif week_choice == "Next week":
        return [(next_week_year, next_week)]
    else:
        return [(current_year, current_week), (next_week_year, next_week)]
    

def get_forecast_weeks_from_date(date_range,mid_date,current_week,current_year):
    forecast_weeks = []
    if date_range[0] < mid_date:
        forecast_weeks.append((current_year, current_week))
    if date_range[1] >= mid_date:
        if current_week+1 > 52:
            forecast_weeks.append((current_year+1,1))
        else:
            forecast_weeks.append((current_year,current_week+1))
    return forecast_weeks