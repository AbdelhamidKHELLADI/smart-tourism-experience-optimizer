
import streamlit as st
from streamlit.components.v1 import html
import os
from utils.data_utils import load_data, categorize_experience, EXPERIENCE_STYLES,render_svg_from_file


st.set_page_config(page_title="Top Regions", layout="wide",page_icon="static/lake.svg")

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


df = load_data(BUCKET_NAME,DATA_PATH)



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
                            Top Regions per Month
                    </h1>
                    <div>{render_svg_from_file('static/chart.svg', height=56, return_html=True)}</div>
                </div>

                    
                <div style="margin-top:10px; color:#31333F; font-family:'Source Sans Pro', sans-serif;">
                    <span style="font-size:17px;"> 
                        
                        Explore the <b>top N regions</b> for a month, filterable by experience level.
                    </span>
                </div>

            </div>""") 



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


st.markdown(f"###  Top {top_n} Regions for {month}")


cols = st.columns(3)

for i, (_, row) in enumerate(top_regions.iterrows()):
    region = row["Region"]
    score = row["tourism_index"]
    level = categorize_experience(score)
    style = EXPERIENCE_STYLES[level]
    bg_color=style['bg']
    icon_path=style['icon']

    with cols[i % 3]:
        # Get the SVG as inline HTML
                    svg_html = render_svg_from_file(icon_path, height=26, return_html=True) if icon_path else ""
                    
          
                    html(
                    f"""
                    <div class='region-card' style='
                            background:{style['bg']};
                            padding:14px;
                            border-radius:12px;
                            height:140px;
                            
                            display:flex;
                            flex-direction:column;
                            justify-content:space-around;  
                            align-items:center;
                            text-align:center;
                            position:relative;
                            font-family: "Source Sans Pro", sans-serif;
                            color:#33333F;
                    '>
                        <div>
                            <h3 style='margin:4px 0; font-size:18px; color:#31333F;'>{row['Region']}</h3>
                            <div style='font-size:14px; color:#333;'>{level.capitalize()}</div>
                        </div>
                        <div style='margin-top:15px;'>{svg_html}</div> <!-- smaller margin -->
                    </div>
                    """,
                    height=175
                )
                    