import pandas as pd

def normalize_text(text):
    if isinstance(text, str):
        for enc in ("latin1", "cp1252"):
            try:
                text = text.encode(enc).decode("utf-8")
                break
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
        text = text.replace("\xa0", " ").strip()
    return text

def merge_calendar_and_exceptions(calendar, calendar_dates):
    calendar = calendar.copy()
    calendar_dates = calendar_dates.copy()

    calendar_dates["date"] = pd.to_datetime(calendar_dates["date"], format="%Y%m%d")
    calendar["start_date"] = pd.to_datetime(calendar["start_date"], format="%Y%m%d")
    calendar["end_date"] = pd.to_datetime(calendar["end_date"], format="%Y%m%d")


    seasonal_services = calendar_dates[~calendar_dates["service_id"].isin(calendar["service_id"])]
    seasonal_services = seasonal_services[seasonal_services["exception_type"] == 1]
    seasonal_services = seasonal_services.assign(
        start_date=seasonal_services["date"],
        end_date=seasonal_services["date"]
    )

    calendar = pd.concat([calendar, seasonal_services], ignore_index=True).drop(columns=["exception_type"])
    return calendar
def expand_dates(df):
    expanded_rows = []
   
    for _, row in df.iterrows():
        start_date = row["start_date"]
        end_date = row["end_date"]
        for single_date in pd.date_range(start_date, end_date):
            weekday_name = single_date.strftime("%A").lower()
            if row.get(weekday_name, 0) == 1:
                new_row = row.copy()
                new_row["date"] = single_date
                expanded_rows.append(new_row)
    return pd.DataFrame(expanded_rows)