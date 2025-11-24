
import streamlit as st
import pandas as pd
import os
import datetime
from utils.data_utils import load_data_cached,get_forecast_weeks,get_forecast_weeks_from_date,EXPERIENCE_STYLES

st.set_page_config(page_title="Weekly forecat", layout="wide",page_icon="🏞️")

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


with st.spinner("Loading forecasts..."):
    df = load_data_cached(BUCKET_NAME,DATA_PATH)

title_emojis={1:"🏔️",2:"🏔️",3: "🏞️",
    4: "🏞️", 5: "🏞️", 6: "🏖️",
    7: "🏖️", 8: "🏖️", 9: "🍂",
    10: "🎃", 11: "🍁", 12: "🏔️"}
st.title(f"Weekly Forecast {title_emojis[CURRENT_MONTH]}")
st.markdown("Best regions to visit each week based on weather and transport availability. ")


tab1, tab2 = st.tabs(["By Week", "By Date"])


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
        for (yr, wk) in forecast_weeks:
            st.markdown(f"### 🗓️ Top Regions for Week {wk}, {yr}")
            start_date = datetime.date.fromisocalendar(yr, wk, 1)
            end_date = datetime.date.fromisocalendar(yr, wk, 7)
            formatted_range = f"{start_date.strftime('%a, %d %b %Y')} – {end_date.strftime('%a, %d %b %Y')}"
            st.markdown(f"##### From {formatted_range}")            
            week_df = filtered_df[(filtered_df["year"] == yr) & (filtered_df["week"] == wk)].sort_values("tourism_index", ascending=False).head(3)
            cols = st.columns(3)
            for i, (_, row) in enumerate(week_df.iterrows()):
                style = EXPERIENCE_STYLES.get(str(row["experience_level"]).title())
                bg_color = style["bg"]
                emoji = style["emoji"]
                with cols[i]:
                    st.markdown(f"""
                        <div class='region-card' style='background-color:{bg_color};'>
                            <h3>{row['Region']}</h3>
                            <div class='experience'>{emoji} {row['experience_level'].capitalize()}</div>
                        </div>
                    """, unsafe_allow_html=True)

with tab2:
    col1, col2 = st.columns(2)
    with col1:
        date_range = st.date_input("Select Date range", value=(START_DATE, END_DATE), min_value=START_DATE, max_value=END_DATE)
        if len(date_range) == 1:
            date_range = (date_range[0], END_DATE)
    with col2:
        region_choice = st.selectbox("Focus on a specific region (optional):", options=["All regions"] + sorted(df["Region"].unique().tolist()),key="r2")

    forecast_weeks=get_forecast_weeks_from_date(date_range,MID_DATE,CURRENT_WEEK,CURRENT_YEAR)
    filtered_df = df[df[["year", "week"]].apply(tuple, axis=1).isin(forecast_weeks)]
    if region_choice != "All regions":
        filtered_df = filtered_df[filtered_df["Region"] == region_choice]

    if filtered_df.empty:
        st.warning("No forecast data available for the selected period.")
    else:
        for (yr, wk) in forecast_weeks:
            st.markdown(f"### 🗓️ Top Regions for Week {wk}, {yr}")
            start_date = datetime.date.fromisocalendar(yr, wk, 1)
            end_date = datetime.date.fromisocalendar(yr, wk, 7)
            formatted_range = f"{start_date.strftime('%a, %d %b %Y')} – {end_date.strftime('%a, %d %b %Y')}"
            st.markdown(f"##### From {formatted_range}")

            week_df = filtered_df[(filtered_df["year"] == yr) & (filtered_df["week"] == wk)].sort_values("tourism_index", ascending=False).head(3)
            cols = st.columns(len(week_df))
            for idx, (_, row) in enumerate(week_df.iterrows()):
                style = EXPERIENCE_STYLES.get(str(row["experience_level"]).title())
                bg_color = style["bg"]
                emoji = style["emoji"]
                with cols[idx]:
                    st.markdown(f"""
                        <div class='region-card' style='background-color:{bg_color};'>
                            <h3>{row['Region']}</h3>
                            <div class='experience'>{emoji} {row['experience_level'].capitalize()}</div>
                        </div>
                    """, unsafe_allow_html=True)


st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 12px; margin-top: 20px;'>
        © 2025 Forecast data generated based on forecasted weather and historical tourism data. <br>
        Developed by <a href="https://github.com/AbdelhamidKHELLADI">Abdelhamid KHELLADI</a>
    </div>
    """,
    unsafe_allow_html=True
)
