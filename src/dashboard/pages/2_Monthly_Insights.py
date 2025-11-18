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
MONTHS = {
    "January":1,    "February":2,  "March":3,
    "April":4,      "May":5,       "June":6,
    "July":7,       "August":8,    "September":9,
    "October":10,   "November":11, "December":12
}
@st.cache_data
def load_forecast():
    df = pd.read_csv(DATA_PATH)
    region_cols=[col for col in df.columns if col.startswith("region_")]
    df["Region"]=df[region_cols].idxmax(axis=1).str.replace("region__","")
    df["Region"]=df["Region"].str.replace('_', " ")
    return df[["Year", "Month_Num","Region", "tourism_index", "experience_level"]]

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


# -----------------------------
# Title
# -----------------------------
st.title("🗓️ Monthly Tourism Insights")

st.markdown("""
Quickly find **the best regions to visit in a selected month**, based on weather conditions and mobility.  

Use the filters below to:
- Pick any month of the year  
- Choose the **Top N regions** to display  
- Filter results by **experience level** (from Quiet Season to Peak Season)
""")
# -----------------------------
# Filters
# -----------------------------
st.markdown("### 🔍 Filter Options")

with st.container():
    st.markdown('<div class="filter-box">', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1,1,1])

    with col1:
        month = st.selectbox("Select Month", MONTHS.keys())

    with col2:
        top_n = st.number_input("Top N Regions", min_value=1, max_value=20, value=5)

    with col3:
        exp_filter = st.multiselect(
            "Filter by Experience Level",
            ["Peak Season","Popular Season","Moderate Season","Quiet Season","Not Ideal"],
            default=[],
            help="Select which experience levels to include in the results."
        )
    
    st.markdown('</div>', unsafe_allow_html=True)

# -----------------------------
# Data Filtering
# -----------------------------
month_num=MONTHS[month]
filtered_df = df[df["Month_Num"] == month_num]

if exp_filter:
    filtered_df = filtered_df[filtered_df["experience_level"].isin(exp_filter)]

# Check empty result
if filtered_df.empty:
    st.warning("No data matches your selected filters.")
    st.stop()

# Compute Top N Regions
top_regions = (
    filtered_df.groupby("Region")["tourism_index"]
    .mean()
    .nlargest(top_n)
    .reset_index()
)

region_details = df[df["Region"].isin(top_regions["Region"])][["Region", "experience_level"]].drop_duplicates()

# -----------------------------
# Display Cards
# -----------------------------
st.markdown(f"### 📊 Top {top_n} Regions for {month}")

cols = st.columns(3)

for i, (idx, row) in enumerate(top_regions.iterrows()):
    region = row["Region"]
    level = categorize_experience(row["tourism_index"])

    
    with cols[i % 3]:
        st.markdown(f"""
        <div class="region-card">
            <h3>{region.replace("_", " ")}</h3>
            <div class="metric-value">{row['tourism_index']:.2f}</div>
            <div class="experience-label">{level}</div>
        </div>
        """, unsafe_allow_html=True)

