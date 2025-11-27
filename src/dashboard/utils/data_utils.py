from utils.s3_utils import  read_from_s3
import streamlit as st
from streamlit.components.v1 import html
import re

EXPERIENCE_STYLES = {
    "Not Ideal": {
        "bg": "#f2f6fa",
        "icon": "static/not_ideal.svg",
    },
    "Quiet Season": {
        "bg": "#f9f9f9",
        "icon": "static/quite.svg",
    },
    "Moderate Season": {
        "bg": "#fff9e6",
        "icon": "static/moderate.svg",
    },
    "Popular Season": {
        "bg": "#e8f5e9",
        "icon": "static/popular.svg",

    },
    "Peak Season": {
        "bg": "#ffe6e6",
        "icon": "static/peak.svg",

    },
}

@st.cache_data(ttl="1d")
def load_forecast(bucket_name,data_path):
    df = read_from_s3(bucket_name, data_path)
    return df[["year", "week", "Region", "tourism_index", "experience_level"]]

@st.cache_data
def load_data(bucket_name,data_path):
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

def render_svg_from_file(svg_path, height=40, return_html=False):
    width = height
    with open(svg_path, "r", encoding="utf-8") as f:
        svg = f.read()

    
    svg = re.sub(r"^\s*<\?xml[^>]*>\s*", "", svg, flags=re.I)

    
    m = re.search(r"<svg([^>]*)>", svg, flags=re.I)
    if m:
        svg_tag = m.group(0)
        svg_attrs = m.group(1)
        if "viewBox" not in svg_attrs:
            wh = re.search(r'width="([\d.]+)"[^>]*height="([\d.]+)"', svg_tag)
            if wh:
                w_val, h_val = wh.group(1), wh.group(2)
                svg = svg.replace(svg_tag, svg_tag.replace("<svg", f"<svg viewBox='0 0 {w_val} {h_val}'"), 1)
                svg_tag = svg_tag
        if "preserveAspectRatio" not in svg_tag:
            svg = svg.replace(svg_tag, svg_tag.replace("<svg", "<svg preserveAspectRatio='xMidYMid meet'"), 1)
        
        svg = svg.replace("<svg", f"<svg style='width:{width}px; height:{height}px;'", 1)

    if return_html:
        return svg
    else:
        iframe_height = max(40, int(height) + 8)
        html(svg, height=iframe_height)


