import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
def extract(year):
    url=f"https://statweb.provincia.tn.it/movturistico/data.asp?db=annuarioturismo&sp=spArrPresEsAlbXAmbProvMes&var=0&a={year}"
    data = requests.get(url)
    soup = BeautifulSoup(data.text, 'html.parser')

    table=soup.find('table') 
    try:
        tables = table.find_all('table')
    except AttributeError:
        print("No table found in the HTML content.")
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
    df.to_csv("tourism_movement.csv", index=False)

def tourism_mouvment():
    current_year = datetime.now().year
    all_data = pd.DataFrame()
    for year in range(2022, current_year+1):
        presance = extract(year)
        if presance is not None:
            df_year = transform(presance, year)
            all_data = pd.concat([all_data, df_year], ignore_index=True)
    load(all_data)

if __name__ == "__main__":
    tourism_mouvment()





