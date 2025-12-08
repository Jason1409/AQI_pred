import numpy as np
import pandas as pd
def build_features(df):
    
    df=df.copy()
    
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    df["doy"] = df["date"].dt.dayofyear
    df["sin_doy"] = np.sin(2 * np.pi * df["doy"] / 365)
    df["cos_doy"] = np.cos(2 * np.pi * df["doy"] / 365)
    
    df["pm2_5_lag1"] = df["pm25_actual"].shift(1)
    df["pm2_5_lag7"] = df["pm25_actual"].shift(7)
    df["pm2_5_ma7"] = df["pm25_actual"].rolling(7).mean()
    df["pm2_5_ma14"] = df["pm25_actual"].rolling(14).mean()
    
    weather_base = [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "surface_pressure",
        "precipitation"
    ]
    
    for col in weather_base:
        if col=="relative_humidity_2m":
            short="humidity_2m"
        else:
            short=col
        df[f"{short}_lag1"] = df[col].shift(1)
        df[f"{short}_lag7"] = df[col].shift(7)
        df[f"{short}_ma7"] = df[col].rolling(7).mean()
        df[f"{short}_ma14"] = df[col].rolling(14).mean()
    return df