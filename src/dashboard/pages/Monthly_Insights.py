import streamlit as st
from streamlit.components.v1 import html
import os
from utils.data_utils import load_data, categorize_experience,EXPERIENCE_STYLES,render_svg_from_file
import calendar

st.set_page_config(page_title="Monthly Insights", layout="wide",page_icon='static/lake.svg')

BUCKET_NAME = os.getenv("TOURISM_BUCKET")
if not BUCKET_NAME:
    raise RuntimeError("TOURISM_BUCKET env var not set (BUCKET_NAME is required)")

DATA_PATH = os.getenv("FORECAST_CSV_PATH", "preprocessed.csv")
MONTH_NAMES = {
    1: "January", 2: "February", 3: "March",
    4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September",
    10: "October", 11: "November", 12: "December"
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
                            Monthly Insights
                    </h1>
                    <div>{render_svg_from_file('static/lake.svg', height=56, return_html=True)}</div>
                </div>

                    
                <div style="margin-top:10px; color:#31333F; font-family:'Source Sans Pro', sans-serif;">
                    <span style="font-size:17px;"> 
                        
                        Discover the <b>best month or season</b> to visit a selected region.
                    </span>
                </div>

            </div>""") 



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


col1,col2=st.columns([0.12,2],gap="small")
with col1:
    render_svg_from_file("static/chart.svg", height=32)
with col2:
    st.markdown(
        f"""
        <div style="display:flex; align-items:center; gap:8px;">
        <h3 style="margin:0;">  Top {search_by} Insights for {region_choice}</h3>
        </div>
        """,
        unsafe_allow_html=True
    )
if search_by == "Month":
    top_month = filtered_df.groupby("Month_Num")["tourism_index"].mean().reset_index()
    month_num = top_month[top_month["tourism_index"]==top_month["tourism_index"].max()]["Month_Num"].iloc[0]
    month_name = calendar.month_name[month_num]
    experience_level = categorize_experience(top_month["tourism_index"].max())
    style = EXPERIENCE_STYLES[experience_level]
    svg_html = render_svg_from_file(style['icon'], height=26, return_html=True) if style['icon'] else ""

    html(
        f"""
        <div class='region-card' style='
            background:{style['bg']};
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
            color:#33333F;
        '>
            <div>
                <h3 style='margin:4px 0; font-size:34px;'>{month_name}</h3>
                <div style='font-size:16px; '>{experience_level.capitalize()}</div>
            </div>
            <div>{svg_html}</div>
        </div>
        """,
        height=160
    )

else:  
    top_season = filtered_df.groupby("season")["tourism_index"].mean().reset_index()
    season_name = top_season[top_season["tourism_index"]==top_season["tourism_index"].max()]["season"].iloc[0]
    experience_level = categorize_experience(top_season["tourism_index"].max())
    style = EXPERIENCE_STYLES[experience_level]


    svg_html = render_svg_from_file(style['icon'], height=26, return_html=True) if style['icon'] else ""
                    
    html(
        f"""
        <div class='region-card' style='
            background:{style['bg']};
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
            color:#33333F;
        '>
            <div>
                <h3 style='margin:4px 0; font-size:34px;'>{season_name}</h3>
                <div style='font-size:16px; '>{experience_level.capitalize()}</div>
            </div>
            <div>{svg_html}</div>
        </div>
        """,
        height=160
    )


