import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from utils.s3_utils import save_to_s3,read_from_s3
BUCKET_NAME=os.getenv("TOURISM_BUCKET")
if not BUCKET_NAME:
    raise RuntimeError("TOURISM_BUCKET env var not set (BUCKET_NAME is required)")

import logging
logging.basicConfig(
    filename='logs/tourism_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
SAVING_PATH = os.getenv("TOURISM_MOVEMENT_PATH", "tourism_movement.csv")


def extract(year):
    url=f"https://statweb.provincia.tn.it/movturistico/data.asp?db=annuarioturismo&sp=spArrPresEsAlbXAmbProvMes&var=0&a={year}"
    data = requests.get(url,timeout=60)
    soup = BeautifulSoup(data.text, 'html.parser')

    table=soup.find('table') 
    try:
        tables = table.find_all('table')
    except AttributeError:
        logging.warning(f"No table found for year {year}")
        return None

    return tables[2]

def transform(presance,year):
    regions=presance.find_all('tr')[0].find_all('td')[1:]
    months_number=len(presance.find_all('tr')[2:])
    df = pd.DataFrame(columns=['Year', 'Month_Num', 'Month_Name', 'Region', 'Italians', 'Foreigners'])
    for month in range(months_number):
        row=presance.find_all('tr')[month+2]
        values=row.find_all('td')[1:]
        values=[int(value.text.replace(".","")) for value in values]
        month_number=month+1
        month_name=row.find_all('td')[0].text.replace("\r\n","").strip()
        if month_name.lower()=="anno":
            month_name="Total"
            month_number=0
        
        df_row=[year,month_number,month_name]
        for i in range(2,len(values),3):
            to_add=df_row.copy()
            region_name=regions[i//3].text
            to_add.append(region_name)
            to_add.append(values[i-2])
            to_add.append(values[i-1])
            df.loc[len(df)] = to_add
    return df

def load(df):
    save_to_s3(df,BUCKET_NAME,SAVING_PATH)

def tourism_mouvment():
    current_year = datetime.now().year
    all_data = pd.DataFrame()
    current_year = datetime.now().year
    all_data = pd.DataFrame()
    changed=False
    try:
        existing_df = read_from_s3(BUCKET_NAME,SAVING_PATH)
        if not existing_df.empty:
            last_year = existing_df["Year"].max()
            if last_year == current_year:
                logging.info(f"Data is already up to date for year {current_year}. No new data to process.")
                return
            logging.info(f"Found existing data up to {last_year}. Resuming from {last_year + 1}")
            all_data = existing_df.copy()
        else:
            last_year = 2021
    except FileNotFoundError:
        logging.info("No existing CSV found, starting from scratch.")
        last_year = 2021

    for year in range(last_year , current_year + 1):
        presance = extract(year)
        if presance is not None:
            df_year = transform(presance, year)
            all_data = pd.concat([all_data, df_year], ignore_index=True)
    all_data.drop_duplicates(inplace=True)
    changed=True
    logging.info(f"Successfully processed data for year {year}")

    load(all_data)

    return changed

if __name__ == "__main__":

    tourism_mouvment()





