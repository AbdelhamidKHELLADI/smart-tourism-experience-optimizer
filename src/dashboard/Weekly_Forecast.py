import streamlit as st
import pandas as pd
import os
import datetime
import re
from streamlit.components.v1 import html,components
from utils.data_utils import load_forecast, get_forecast_weeks, get_forecast_weeks_from_date, EXPERIENCE_STYLES,render_svg_from_file
import re


# ---------- Config ----------
st.set_page_config(page_title="Weekly Forecast", layout="wide", page_icon="static/lake.svg")

BUCKET_NAME = os.getenv("TOURISM_BUCKET")
if not BUCKET_NAME:
    raise RuntimeError("TOURISM_BUCKET env var not set (BUCKET_NAME is required)")

DATA_PATH = os.getenv("FORECAST_CSV_PATH", "predictions.csv")

TODAY = datetime.datetime.now()
CURRENT_WEEK = TODAY.isocalendar().week
CURRENT_MONTH = TODAY.month
CURRENT_YEAR = TODAY.isocalendar().year
START_DATE = datetime.date.fromisocalendar(CURRENT_YEAR, CURRENT_WEEK, 1)
END_DATE = datetime.date.fromisocalendar(CURRENT_YEAR, CURRENT_WEEK + 1 if CURRENT_WEEK < 52 else 1, 7)
MID_DATE = datetime.date.fromisocalendar(CURRENT_YEAR, CURRENT_WEEK + 1 if CURRENT_WEEK < 52 else 1, 1)

# ---------- Styles ----------
st.markdown("""
<style>
.region-card {
    margin-bottom: 10px;
    padding: 20px;
    border-radius: 15px;
    text-align: center;
}
.region-card h3, .region-card .experience { color: black; }


</style>
""", unsafe_allow_html=True)

# ---------- Load Data ----------
with st.spinner("Loading forecasts..."):
    df = load_forecast(BUCKET_NAME, DATA_PATH)


# ---------- Title ----------
title_icons = {
    1: "static/mountain.svg", 2: "static/mountain.svg", 3: "static/lake.svg",
    4: "static/lake.svg", 5: "static/lake.svg", 6: "static/lake.svg",
    7: "static/lake.svg", 8: "static/lake.svg", 9: "static/leaf.svg",
    10: "static/pumpkin.svg", 11: "static/leaf.svg", 12: "static/mountain.svg"
}
import re
from streamlit.components.v1 import html as st_html



if CURRENT_MONTH in title_icons:
    html(f"""
                <div style="
                    font-family:'Source Sans Pro', sans-serif;
                    color:#31333F;
                ">

                    <!-- Row 1: icon + title -->
                <div style="
                        display:flex;
                        align-items:center;
                        gap:10px;
                ">
                        
                    <h1 style="margin:0;">
                            Weekly Forecast
                    </h1>
                    <div>{render_svg_from_file('static/leaf.svg', height=56, return_html=True)}</div>
                </div>

                    
                <div style="margin-top:1px; color:#31333F; font-family:'Source Sans Pro', sans-serif;">
                    <span style="font-size:14px;"> 
                        Best regions to visit each week based on weather and transport availability.
                    </span>
                </div>

            </div>""") 
else:
    st.title("Weekly Forecast")


tab1, tab2 = st.tabs(["By Week", "By Date"])

# ---------- Tab 1: By Week ----------
with tab1:
    col1, col2 = st.columns(2)
    with col1:
        week_choice = st.selectbox("Select forecast period:", ["This week", "Next week", "Both"], index=0)
    with col2:
        region_choice = st.selectbox("Focus on a specific region (optional):", ["All regions"] + sorted(df["Region"].unique().tolist()))

    forecast_weeks = get_forecast_weeks(CURRENT_WEEK, CURRENT_YEAR, week_choice)
    filtered_df = df[df[["year", "week"]].apply(tuple, axis=1).isin(forecast_weeks)]
    if region_choice != "All regions":
        filtered_df = filtered_df[filtered_df["Region"] == region_choice]

    if filtered_df.empty:
        st.warning("No forecast data available for the selected period.")
    else:
        for yr, wk in forecast_weeks:
            col1,col2,*_=st.columns([0.12,1,0.69],gap=None)
            with col1:
                render_svg_from_file("static/calendar.svg", height=36)
            with col2:
                st.markdown(
                    f"""
                    <div style="display:flex; align-items:center; gap:8px;">
                    <h3 style="margin:0;">Top Regions for Week {wk}, {yr}</h3>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            start_date = datetime.date.fromisocalendar(yr, wk, 1)
            end_date = datetime.date.fromisocalendar(yr, wk, 7)
            st.markdown(f"##### From {start_date.strftime('%a, %d %b %Y')} – {end_date.strftime('%a, %d %b %Y')}")


            
            week_df = filtered_df[(filtered_df["year"] == yr) & (filtered_df["week"] == wk)].sort_values("tourism_index", ascending=False).head(3)
            cols = st.columns(len(week_df))
            for i, (_, row) in enumerate(week_df.iterrows()):
                style = EXPERIENCE_STYLES.get(str(row["experience_level"]).title(), {})
                bg_color = style.get("bg", "transparent")
                icon_path = style.get("icon", None)

                with cols[i]:

                    
                    # Get the SVG as inline HTML
                    svg_html = render_svg_from_file(icon_path, height=26, return_html=True) if icon_path else ""
                    
                    # Render the full card in one markdown block
                    html(
                        f"""
                        <div class='region-card' style='
                            background:{bg_color};
                            padding:14px;
                            border-radius:12px;
                            height:120px;
                            display:flex;
                            flex-direction:column;
                            justify-content:space-around;  
                            align-items:center;
                            text-align:center;
                            position:relative;
                            font-family: "Source Sans Pro", sans-serif;
                        '>
                            <div>
                                <h3 style='margin:4px 0; font-size:18px; color:#000;'>{row['Region']}</h3>
                                <div style='font-size:16px; color:#333;'>{row['experience_level'].capitalize()}</div>
                            </div>
                            <div>{svg_html}</div>
                        </div>
                        """,
                        height=160
                    )


                                
# ---------- Tab 2: By Date ----------
with tab2:
    col1, col2 = st.columns(2)
    with col1:
        date_range = st.date_input("Select Date range", value=(START_DATE, END_DATE), min_value=START_DATE, max_value=END_DATE)
        if len(date_range) == 1:
            date_range = (date_range[0], END_DATE)
    with col2:
        region_choice = st.selectbox("Focus on a specific region (optional):", ["All regions"] + sorted(df["Region"].unique().tolist()), key="r2")

    forecast_weeks = get_forecast_weeks_from_date(date_range, MID_DATE, CURRENT_WEEK, CURRENT_YEAR)
    filtered_df = df[df[["year", "week"]].apply(tuple, axis=1).isin(forecast_weeks)]
    if region_choice != "All regions":
        filtered_df = filtered_df[filtered_df["Region"] == region_choice]

    if filtered_df.empty:
        st.warning("No forecast data available for the selected period.")
    else:
        for yr, wk in forecast_weeks:
            col1,col2,*_=st.columns([0.05,0.3,0.69],gap=None)
            with col1:
                render_svg_from_file("static/calendar.svg", height=36)
            with col2:
                st.markdown(
                    f"""
                    <div style="display:flex; align-items:center; gap:8px;">
                    <h3 style="margin:0;">Top Regions for Week {wk}, {yr}</h3>
                    </div>
                    """,
                    unsafe_allow_html=True
                )


            start_date = datetime.date.fromisocalendar(yr, wk, 1)
            end_date = datetime.date.fromisocalendar(yr, wk, 7)
            st.markdown(f"##### From {start_date.strftime('%a, %d %b %Y')} – {end_date.strftime('%a, %d %b %Y')}")

            week_df = filtered_df[(filtered_df["year"] == yr) & (filtered_df["week"] == wk)].sort_values("tourism_index", ascending=False).head(3)
            cols = st.columns(len(week_df))
            for i, (_, row) in enumerate(week_df.iterrows()):
                style = EXPERIENCE_STYLES.get(str(row["experience_level"]).title(), {})
                bg_color = style.get("bg", "transparent")
                icon_path = style.get("icon", None)

                with cols[i]:
                    
                    svg_html = render_svg_from_file(icon_path, height=26, return_html=True) if icon_path else ""
                    
                    html(
                        f"""
                        <div class='region-card' style='
                            background:{bg_color};
                            padding:14px;
                            border-radius:12px;
                            height:120px;
                            display:flex;
                            flex-direction:column;
                            justify-content:space-around;  
                            align-items:center;
                            text-align:center;
                            position:relative;
                            font-family: "Source Sans Pro", sans-serif;
                        '>
                            <div>
                                <h3 style='margin:4px 0; font-size:18px; color:#000;'>{row['Region']}</h3>
                                <div style='font-size:16px; color:#333;'>{row['experience_level'].capitalize()}</div>
                            </div>
                            <div>{svg_html}</div>
                        </div>
                        """,
                        height=160
                    )

st.markdown("---")

st.markdown("""
<div style='text-align: center; color: gray; font-size: 12px; margin-top: 20px;'>
    © 2025 Forecast data generated based on forecasted weather and historical tourism data. <br>
    Developed by <a href="https://github.com/AbdelhamidKHELLADI">Abdelhamid KHELLADI</a>
</div>
""", unsafe_allow_html=True)
