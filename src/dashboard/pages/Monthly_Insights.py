import streamlit as st
import os
from utils.data_utils import load_forecast, categorize_experience,EXPERIENCE_STYLES
import calendar

st.set_page_config(page_title="Monthly Insights", layout="wide",page_icon='🏞️')


DATA_PATH = os.getenv("FORECAST_CSV_PATH", "data/preprocessed.csv")
MONTH_NAMES = {
    1: "January", 2: "February", 3: "March",
    4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September",
    10: "October", 11: "November", 12: "December"
}


df = load_forecast(DATA_PATH)



st.title("Monthly Insights 🏞️")
st.markdown("Discover the **best month or season** to visit a selected region.")


st.markdown("### 🔍 Filter Options")

with st.container():

    col1, col2 = st.columns([1, 1], gap="large")

    with col1:
        region_choice = st.selectbox(
            "Region:",
            ["All regions"] + sorted(df["Region"].unique().tolist()),
            help="Select the region for which you want to see tourism insights."
        )

    with col2:
        search_by = st.selectbox(
            "Search by:",
            ['Month','Season'],
            help="Choose whether to find the best month or season to visit the selected region."
        )



if region_choice=="All regions":
    st.warning("Select a region first")
    st.stop()

filtered_df = df[df["Region"] == region_choice]
if filtered_df.empty:
    st.warning("No data matches your selected filters.")
    st.stop()

st.markdown(f"### 📊 Top {search_by} Insights for {region_choice}")

if search_by == "Month":
    top_month = filtered_df.groupby("Month_Num")["tourism_index"].mean().reset_index()
    month_num = top_month[top_month["tourism_index"]==top_month["tourism_index"].max()]["Month_Num"].iloc[0]
    month_name = calendar.month_name[month_num]
    experience_level = categorize_experience(top_month["tourism_index"].max())
    style = EXPERIENCE_STYLES[experience_level]

    st.markdown(f"""
        <div style="padding:20px; border-radius:15px; background-color:{style['bg']}; text-align:center;">
            <h2>{month_name}</h2>
            <p>{style['emoji']} {experience_level}</p>
        </div>
    """, unsafe_allow_html=True)

else:  
    top_season = filtered_df.groupby("season")["tourism_index"].mean().reset_index()
    season_name = top_season[top_season["tourism_index"]==top_season["tourism_index"].max()]["season"].iloc[0]
    experience_level = categorize_experience(top_season["tourism_index"].max())
    style = EXPERIENCE_STYLES[experience_level]

    st.markdown(f"""
        <div style="padding:20px; border-radius:15px; background-color:{style['bg']}; text-align:center;">
            <h2>{season_name}</h2>
            <p>{style['emoji']} {experience_level}</p>
        </div>
    """, unsafe_allow_html=True)
