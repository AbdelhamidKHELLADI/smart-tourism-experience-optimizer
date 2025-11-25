
import streamlit as st
import os
from utils.data_utils import load_forecast, categorize_experience, EXPERIENCE_STYLES


st.set_page_config(page_title="Top Regions", layout="wide",page_icon="🏞️")

BUCKET_NAME = os.getenv("TOURISM_BUCKET")
if not BUCKET_NAME:
    raise RuntimeError("TOURISM_BUCKET env var not set (BUCKET_NAME is required)")

DATA_PATH = os.getenv("FORECAST_CSV_PATH", "preprocessed.csv")
MONTHS = {
    "January":1, "February":2, "March":3,
    "April":4, "May":5, "June":6,
    "July":7, "August":8, "September":9,
    "October":10, "November":11, "December":12
}


df = load_forecast(BUCKET_NAME,DATA_PATH)


st.title("Top Regions per Month 📊 ")
st.markdown("Explore the **top N regions** for a month, filterable by experience level.")


st.markdown("### 🔍 Filter Options")

with st.container():
    

    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        month = st.selectbox("Select Month", MONTHS.keys())
    with col2:
        top_n = st.number_input("Top N Regions", min_value=1, max_value=12, value=5)
    with col3:
        exp_filter = st.multiselect(
            "Filter by Experience Level",
            reversed(list(EXPERIENCE_STYLES.keys())),
            default=[],
            help="Select which experience levels to include in the results."
        )



month_num = MONTHS[month]
filtered_df = df[df["Month_Num"] == month_num]

if exp_filter:
    filtered_df = filtered_df[filtered_df["experience_level"].isin(exp_filter)]

if filtered_df.empty:
    st.warning("No data matches your selected filters.\n\n Try changing the Month or Experience Level filter.")
    st.stop()

top_regions = (
    filtered_df.groupby("Region")["tourism_index"]
    .mean()
    .nlargest(top_n)
    .reset_index()
)


st.markdown(f"### 📊 Top {top_n} Regions for {month}")


cols = st.columns(3)

for i, (_, row) in enumerate(top_regions.iterrows()):
    region = row["Region"]
    score = row["tourism_index"]
    level = categorize_experience(score)
    style = EXPERIENCE_STYLES[level]

    with cols[i % 3]:
        st.markdown(f"""
        <div style="
            padding: 20px;
            border-radius: 15px;
            background-color: {style['bg']};
            text-align: center;
            margin-bottom: 15px;
        ">
            <h3>{region}</h3>
            <p>Tourism Index: {score:.2f}</p>
            <p>{style['emoji']} {level}</p>
        </div>
        """, unsafe_allow_html=True)
