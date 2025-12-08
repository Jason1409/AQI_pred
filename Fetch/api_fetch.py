import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from datetime import datetime
from datetime import date as dt_date, timedelta
import pandas as pd

def fetch_and_insert_raw(target_date: dt_date | None = None):
    """
    Fetch pollutants + weather for a single date and insert into DB.
    If target_date is None, uses today.
    """
    if target_date is None:
        target_date = dt_date.today()

    print("Fetching for:", target_date)

    pol = fetch_pollutants_for_day(target_date)
    weather = fetch_weather_for_day(target_date)

    print("POL:", pol)
    print("WEATHER:", weather)

    if pol is None or weather is None:
        print(" No data. Skipping insert for", target_date)
        return

    row = {
        "date": target_date,
        "pm25_actual": float(pol["pm2_5"]),
        "no2": float(pol["no2"]),
        "co": float(pol["co"]),

        "temperature_2m": float(weather["temperature_2m"]),
        "relative_humidity_2m": float(weather["relative_humidity_2m"]),
        "wind_speed_10m": float(weather["wind_speed_10m"]),
        "surface_pressure": float(weather["surface_pressure"]),
        "precipitation": float(weather["precipitation"]),
    }

    print("ROW TO INSERT:", row)
    insert_raw(row)
    print(" Inserted successfully for:", target_date)


from DB.aqi_db import insert_raw
LOCATIONS = [
    ("Silk Board", 12.917, 77.623),
    ("Koramangala", 12.936, 77.622),
    ("Indiranagar", 12.971, 77.640),
    ("Electronic City", 12.844, 77.675),
    ("Whitefield", 12.969, 77.750),
    ("Yeshwanthpur", 13.024, 77.540),
]

session = requests_cache.CachedSession('.cache', expire_after=3600)
session = retry(session, retries=3, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client()

def fetch_weather_for_day(date):
    rows = []

    for name, lat, lon in LOCATIONS:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date,
            "end_date": date,
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "surface_pressure",
                "wind_speed_10m"
            ]
        }

        response = openmeteo.weather_api(
            "https://archive-api.open-meteo.com/v1/era5",
            params
        )[0]

        hourly = response.Hourly()
        df = pd.DataFrame({
            "time": pd.to_datetime(hourly.Time(), unit="s"),
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
            "precipitation": hourly.Variables(2).ValuesAsNumpy(),
            "surface_pressure": hourly.Variables(3).ValuesAsNumpy(),
            "wind_speed_10m": hourly.Variables(4).ValuesAsNumpy(),
        })
        df["date"] = df["time"].dt.date
        daily = df.groupby("date").mean(numeric_only=True)
        rows.append(daily.iloc[0])

    avg = pd.DataFrame(rows).mean(numeric_only=True)
    avg["date"] = date
    return avg.to_dict()


def fetch_pollutants_for_day(date):
    rows = []

    for name,lat, lon in LOCATIONS:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date,
            "end_date": date,
            "hourly": ["pm2_5", "carbon_monoxide", "nitrogen_dioxide"],
        }

        response = openmeteo.weather_api(
            "https://air-quality-api.open-meteo.com/v1/air-quality",
            params
        )[0]   # one response per call

        hourly = response.Hourly()
        df = pd.DataFrame({
            "time": pd.to_datetime(hourly.Time(), unit="s"),
            "pm2_5": hourly.Variables(0).ValuesAsNumpy(),
            "co": hourly.Variables(1).ValuesAsNumpy(),
            "no2": hourly.Variables(2).ValuesAsNumpy(),
        })
        df["date"] = df["time"].dt.date
        daily = df.groupby("date").mean(numeric_only=True)
        rows.append(daily.iloc[0])

    avg = pd.DataFrame(rows).mean(numeric_only=True)
    avg["date"] = date
    return avg.to_dict()



def fetch_and_insert_raw(target_date: dt_date | None = None):
    """
    Fetch pollutants + weather for a single date and insert into DB.
    If target_date is None, uses today.
    """
    if target_date is None:
        target_date = dt_date.today()

    print("Fetching for:", target_date)

    pol = fetch_pollutants_for_day(target_date)
    weather = fetch_weather_for_day(target_date)

    print("POL:", pol)
    print("WEATHER:", weather)

    if pol is None or weather is None:
        print(" No data. Skipping insert for", target_date)
        return

    row = {
        "date": target_date,
        "pm25_actual": float(pol["pm2_5"]),
        "no2": float(pol["no2"]),
        "co": float(pol["co"]),

        "temperature_2m": float(weather["temperature_2m"]),
        "relative_humidity_2m": float(weather["relative_humidity_2m"]),
        "wind_speed_10m": float(weather["wind_speed_10m"]),
        "surface_pressure": float(weather["surface_pressure"]),
        "precipitation": float(weather["precipitation"]),
    }

    print("ROW TO INSERT:", row)
    insert_raw(row)
    print(" Inserted successfully for:", target_date)
