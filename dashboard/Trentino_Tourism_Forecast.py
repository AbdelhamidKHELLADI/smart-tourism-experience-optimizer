import streamlit as st
import pandas as pd
import os
import datetime
from utils.s3_utils import read_from_s3

# -----------------------------
# Config
# -----------------------------
st.set_page_config(
    page_title="Tourism Forecast Overview",
    layout="wide",
    initial_sidebar_state="expanded"
)

BUCKET_NAME=os.getenv("TOURISM_BUCKET")
if not BUCKET_NAME:
    raise RuntimeError("TOURISM_BUCKET env var not set (BUCKET_NAME is required)")


DATA_PATH = os.getenv("FORECAST_CSV_PATH", "predictions.csv")
CURRENT_WEEK = datetime.datetime.now().isocalendar().week
CURRENT_YEAR = datetime.datetime.now().isocalendar().year
START_DATE=datetime.date.fromisocalendar(CURRENT_YEAR, CURRENT_WEEK, 1)
END_DATE=datetime.date.fromisocalendar(CURRENT_YEAR, CURRENT_WEEK+1, 7)
MID_DATE=datetime.date.fromisocalendar(CURRENT_YEAR, CURRENT_WEEK+1, 1)
@st.cache_data
def loading():
    df = read_from_s3(BUCKET_NAME,DATA_PATH)
    # Expected columns: ['year','week','Region','tourism_index','experience_level']
    return df[["year", "week", "Region", "tourism_index", "experience_level"]]

df = loading()

experience_styles = {
    "Not ideal": {"color": "#f2f6fa", "emoji": "😴"},
    "Quiet season": {"color": "#f9f9f9", "emoji": "🛌"},
    "Moderate season": {"color": "#fff9e6", "emoji": "🙂"},
    "Popular": {"color": "#e8f5e9", "emoji": "😎"},
    "Peak": {"color": "#ffe6e6", "emoji": "🔥"},
}



st.markdown("""
<style>
.region-card {
    margin-bottom: 10px;
    padding: 20px;
    border-radius: 15px;
    text-align: center;
}

/* Only text gets adaptive readability, emojis stay normal */
.region-card h3,
.region-card .experience {
    color: black;
    text-shadow: 
        1px 1px 0 rgba(255,255,255,0.6),
        -1px -1px 0 rgba(255,255,255,0.6);
}
</style>
""", unsafe_allow_html=True)


# -----------------------------
# Page Header
# -----------------------------
st.title("🏔️ Weekly Tourism Forecast — Best Regions to Visit")

st.markdown("""
Quickly see which regions offer the **best conditions to visit** based on weather and transport availability.  

- View the **Top 3 recommended regions** each week  
- Compare forecasts between weeks  
- Check the forecast for a **specific region**

""")


# -----------------------------
# Forecast Controls (on-page)
# -----------------------------
st.markdown("## 🎯 Choose Your Forecast Options")

mode = st.radio(
    "Search Mode:",
    ["By Week", "By Date"],
    horizontal=True
)
if mode == "By Week":
    col1, col2 = st.columns(2)

    with col1:
        week_choice = st.selectbox(
            "Select forecast period:",
            ["This week", "Next week", "Both"],
            index=0,
            help="choose which week(s) to view"
        )

    with col2:
        region_choice = st.selectbox(
            "Focus on a specific region (optional):",
            ["All regions"] + sorted(df["Region"].unique().tolist()),
            help="leave as 'All regions' to see top performers"
        )
    
    def get_forecast_weeks(current_week, current_year,week_choice):
        next_week = current_week +1
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

    forecast_weeks = get_forecast_weeks(CURRENT_WEEK, CURRENT_YEAR,week_choice=week_choice)


else:
    col1, col2 = st.columns(2)

    with col1:
        date_range = st.date_input("Select Date range",
                                    value=(START_DATE, END_DATE),
                                    min_value=START_DATE,
                                    max_value=END_DATE)
        if len(date_range) == 1:
            date_range = (date_range[0], END_DATE)


    with col2:
        region_choice = st.selectbox(
            "Focus on a specific region (optional):",
            ["All regions"] + sorted(df["Region"].unique().tolist()),
            help="leave as 'All regions' to see top performers"
        )
    
    forecast_weeks=[]
    if date_range[0] < MID_DATE:
        forecast_weeks.append((CURRENT_YEAR,CURRENT_WEEK))
    if date_range[1] >= MID_DATE:
        if CURRENT_WEEK+1 > 52:
            forecast_weeks.append((CURRENT_YEAR+1,1))
        else:
            forecast_weeks.append((CURRENT_YEAR,CURRENT_WEEK+1))




filtered_df = df[df[["year", "week"]].apply(tuple, axis=1).isin(forecast_weeks)]

if region_choice != "All regions":
    filtered_df = filtered_df[filtered_df["Region"] == region_choice]


# -----------------------------
# Main Content
# -----------------------------
if filtered_df.empty:
    st.warning("No forecast data available for the selected period.")
else:
    if region_choice == "All regions":
        for (yr, wk) in forecast_weeks:
            st.markdown(f"### 🗓️ Top Regions for Week {wk}, {yr}")
            st.markdown(f"#### From {datetime.date.fromisocalendar(yr, wk, 1)} to {datetime.date.fromisocalendar(yr, wk, 7)}")

            week_df = (
                filtered_df[(filtered_df["year"] == yr) & (filtered_df["week"] == wk)]
                .sort_values("tourism_index", ascending=False)
                .head(3)
            )
            
            cols = st.columns(3)
            for i, (_, row) in enumerate(week_df.iterrows()):
                style = experience_styles.get(str(row["experience_level"]).capitalize())
                
                bg_color = style["color"]
                emoji = style["emoji"]
                
                with cols[i]:
                    st.markdown(f"""
                    <div class="region-card" style="background-color:{bg_color};">
                        <h3>{row['Region']}</h3>
                        <div class="experience">{row["tourism_index"]}</div>
                        <div class="experience">{emoji} {row['experience_level'].capitalize()}</div>
                    </div>
                    """, unsafe_allow_html=True)
            

            st.markdown("<br>", unsafe_allow_html=True)
    else:
        st.markdown(f"### 📍 Tourism Forecast for **{region_choice}**")

        # Keep rows for the selected region and sort chronologically
        region_df = (
            filtered_df[filtered_df["Region"] == region_choice]
            .sort_values(["year", "week"])
            .reset_index(drop=True)
        )

        # Create columns: 1 column per forecast week
        cols = st.columns(len(forecast_weeks))

        # Loop through weeks and fill each column
        for idx, (yr, wk) in enumerate(forecast_weeks):
            sub = region_df[(region_df["year"] == yr) & (region_df["week"] == wk)]

            if sub.empty:
                with cols[idx]:
                    st.warning(f"No forecast for Week {wk}, {yr}.")
                continue

            row = sub.iloc[0]

            experience = (
                str(row["experience_level"]).capitalize()
                if pd.notna(row.get("experience_level"))
                else "Unknown"
            )

            style = experience_styles.get(experience, {"color": "#ffffff", "emoji": ""})
            bg_color = style["color"]
            emoji = style["emoji"]

            # Write card inside the column
            with cols[idx]:
                st.markdown(f"""
                <div class="region-card" style="background-color:{bg_color};">
                    <h3>Week {wk}, {yr}</h3>
                    <div class="experience">{emoji} {experience}</div>
                </div>
                """, unsafe_allow_html=True)

    


st.markdown("---")
st.caption("© 2025 Tourism Analytics Dashboard — Forecast data generated by ML models.")
