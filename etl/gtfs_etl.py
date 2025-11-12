import os
import pandas as pd
import geopandas as gpd
from utils.gtfs_utils import normalize_text, merge_calendar_and_exceptions, expand_dates
import json


PATH = os.getenv("GTFS_DATA_PATH")
GEO_PATH = os.getenv("GEO_BOUNDARY_PATH")

def load_gtfs_data(path=PATH, geo_path=GEO_PATH):
    routes = pd.read_csv(f"{path}/routes.txt")
    trips = pd.read_csv(f"{path}/trips.txt")
    calendar = pd.read_csv(f"{path}/calendar.txt")
    calendar_dates = pd.read_csv(f"{path}/calendar_dates.txt")
    stops = pd.read_csv(f"{path}/stops.txt")
    stop_times = pd.read_csv(f"{path}/stop_times.txt")
    regions = gpd.read_file(geo_path)

    return {
        "routes": routes,
        "trips": trips,
        "calendar": calendar,
        "calendar_dates": calendar_dates,
        "stops": stops,
        "stop_times": stop_times,
        "regions": regions
    }


def process_gtfs_data(data):
    merged_calendar = merge_calendar_and_exceptions(data["calendar"], data["calendar_dates"])
    trips_routes_and_calendar = pd.merge(data["trips"], merged_calendar, on="service_id", how="left")
    
    expanded = expand_dates(trips_routes_and_calendar)

    regions = data["regions"]
    stops = data["stops"]
    stops_gdf = gpd.GeoDataFrame(
        stops,
        geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat),
        crs="EPSG:4326"
    )
    regions = regions.to_crs("EPSG:4326")
    stops_with_regions = gpd.sjoin(stops_gdf, regions, how="left", predicate="within")

    with open("utils/comune_to_region_map.json", "r", encoding="utf-8") as f:
        comune_to_region = json.load(f)

    stops_with_regions["tourism_region"] = stops_with_regions["COMUNE"].apply(normalize_text).map(comune_to_region).fillna("Unknown")
    regions = stops_with_regions['tourism_region'].unique().tolist()
    geo_data = {}
    for region in regions:
        region_df = stops_with_regions[stops_with_regions['tourism_region'] == region]
        max_lon = region_df['stop_lon'].max()
        min_lon = region_df['stop_lon'].min()
        max_lat = region_df['stop_lat'].max()
        min_lat = region_df['stop_lat'].min()
        geo_data[region] = {
            'max_lon': float(max_lon),
            'min_lon': float(min_lon),
            'max_lat': float(max_lat),
            'min_lat': float(min_lat)
        }
    
    stop_times = data["stop_times"]
    stop_times_with_regions = pd.merge(stop_times, stops_with_regions[["stop_id", "tourism_region"]], on="stop_id", how="left")

    trips_stops_regions = pd.merge(trips_routes_and_calendar, stop_times_with_regions[["trip_id", "tourism_region"]].drop_duplicates(), on="trip_id", how="left")
    trips_stops_regions = trips_stops_regions[trips_stops_regions["tourism_region"] != "Unknown"]
    trips_stops_regions = trips_stops_regions.drop(
        columns=[
            'shape_id', 'route_id', 'trip_headsign', 'direction_id',
            'service_id', 'start_date', 'end_date',
            'monday', 'tuesday', 'wednesday', 'thursday',
            'friday', 'saturday', 'sunday'
        ],
        errors='ignore' 
    )
    final_df = pd.merge(
        expanded,
        trips_stops_regions[["trip_id", "tourism_region"]].drop_duplicates(),
        on="trip_id",
        how="left"
    )
    final_df["date"] = pd.to_datetime(final_df["date"])

    monthly_trips = (
        final_df.groupby(["tourism_region", final_df["date"].dt.to_period("M")])["trip_id"]
        .nunique()
        .reset_index(name="num_trips")
    )

    return monthly_trips, geo_data


def add_mobility_index(tourism_movement_path, monthly_trips):
    tourism_movement = pd.read_csv(tourism_movement_path)
    monthly_trips['month'] = monthly_trips['date'].dt.month
    merged = pd.merge(tourism_movement, monthly_trips, left_on=['Region', 'Month_Num'], right_on=['tourism_region', 'month'], how='left')
    merged.drop(columns=['tourism_region', 'month', 'date'], inplace=True)
    july_missing = merged[(merged['Month_Num'] == 7) & (merged['num_trips'].isna())]
    for idx, row in july_missing.iterrows():
        region = row['Region']
        june_value = merged.loc[(merged['Region'] == region) & (merged['Month_Num'] == 6), 'num_trips'].values
        if len(june_value) > 0:
            merged.at[idx, 'num_trips'] = june_value[0]
    august_missing = merged[(merged['Month_Num'] == 8) & (merged['num_trips'].isna())]
    for idx, row in august_missing.iterrows():
        region = row['Region']
        sept_value = merged.loc[(merged['Region'] == region) & (merged['Month_Num'] == 9), 'num_trips'].values
        if len(sept_value) > 0:
            merged.at[idx, 'num_trips'] = sept_value[0]
    merged.to_csv("data/tourism_movement_with_gtfs.csv", index=False)


def main():
    gtfs_data = load_gtfs_data(PATH)
    monthly_trips, regions_with_boundries = process_gtfs_data(gtfs_data)
    add_mobility_index("data/tourism_movement.csv", monthly_trips)
    json_path = "data/regions_boundries.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(regions_with_boundries, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    main()
