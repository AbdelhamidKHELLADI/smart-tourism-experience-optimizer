import streamlit as st
import pandas as pd
import os

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(page_title="Monthly Insights", layout="wide")

# -----------------------------
# Load data
# -----------------------------
DATA_PATH = os.getenv("FORECAST_CSV_PATH", "data/preprocessed.csv")
MONTH_NAMES = {
    1: "January",    2: "February",  3: "March",
    4: "April",      5: "May",       6: "June",
    7: "July",       8: "August",    9: "September",
    10: "October",  11: "November", 12: "December"
}

@st.cache_data
def load_forecast():
    df = pd.read_csv(DATA_PATH)
    region_cols=[col for col in df.columns if col.startswith("region_")]
    df["Region"]=df[region_cols].idxmax(axis=1).str.replace("region__","")
    df["Region"]=df["Region"].str.replace('_', " ")
    return df[["Year", "Month_Num","Region", "season", "tourism_index", "experience_level"]]

df = load_forecast()

def categorize_experience(score):
    if score < 0.1:
        return "Not Ideal"
    elif score < 0.4:
        return "Quiet Season"
    elif score < 0.6:
        return "Moderate Season"
    elif score < 0.75:
        return "Popular Season"
    else:
        return "Peak Season"


st.title("🏞️ Trentino Tourism Insights — Best Time to Visit")


st.markdown("""
Quickly discover **which months or seasons are best to visit your selected region**, based on:
- Weather suitability
- Mobility and transport conditions

Use the filters below to:
- Select a **Region**
- Choose whether to search by **Month** or **Season**
- Identify the top experience period for the best visit

""")

st.markdown("### 🔍 Filter Options")

with st.container():
    st.markdown('<div class="filter-box">', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)

    with col1:
        region_choice = st.selectbox(
        "Region:",
        ["All regions"] + sorted(df["Region"].unique().tolist()),
        help="Select the region for which you want to see tourism insights."
    )
    

    with col2:
        search_by = st.selectbox("search by",['Month','Season'],
                                 help="Choose whether to find the best month or season to visit the selected region."
                                 )


    
    st.markdown('</div>', unsafe_allow_html=True)



filtered_df = df[df["Region"] == region_choice]

if region_choice=="All regions":
    st.warning("Select a region first")
    st.stop()    
if filtered_df.empty:
    st.warning("No data matches your selected filters.")
    st.stop()

if search_by=="Month":
    top_month = (
        filtered_df.groupby("Month_Num")["tourism_index"]
        .mean()
        .reset_index()
    )
    month_num=top_month[top_month["tourism_index"]==top_month["tourism_index"].max()]["Month_Num"].iloc[0]
    month=MONTH_NAMES[month_num]



    # -----------------------------
    # Display Cards
    # -----------------------------
    st.markdown(f"### 📊 Top {search_by} Regions for {region_choice}")


    st.markdown(f"""
        <h3>{month}</h3>
    """, unsafe_allow_html=True)

else:

    top_season= (
        filtered_df.groupby("season")["tourism_index"]
        .mean()
        .reset_index()
    )
    season=top_season[top_season["tourism_index"]==top_season["tourism_index"].max()]["season"].iloc[0]
   



    # -----------------------------
    # Display Cards
    # -----------------------------
    st.markdown(f"### 📊 Top {search_by} Regions for {region_choice}")


    st.markdown(f"""
        <h3>{season}</h3>
    """, unsafe_allow_html=True)
