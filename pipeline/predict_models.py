import os
from tomlkit import date
import xgboost as xgb

# Feature names expected by model 1
model1_features = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "surface_pressure",
    "precipitation",
    "temperature_2m_lag1",
    "humidity_2m_lag1",
    "wind_speed_10m_lag1",
    "surface_pressure_lag1",
    "precipitation_lag1",
    "temperature_2m_lag7",
    "humidity_2m_lag7",
    "wind_speed_10m_lag7",
    "surface_pressure_lag7",
    "precipitation_lag7",
    "temperature_2m_ma7",
    "humidity_2m_ma7",
    "wind_speed_10m_ma7",
    "surface_pressure_ma7",
    "precipitation_ma7",
    "temperature_2m_ma14",
    "humidity_2m_ma14",
    "wind_speed_10m_ma14",
    "surface_pressure_ma14",
    "precipitation_ma14",
    "sin_doy",
    "cos_doy"
]


# Feature names expected by model 2
model2_features = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "surface_pressure",
    "precipitation",
    "pm2_5_lag1",
    "pm2_5_lag7",
    "pm2_5_ma7",
    "pm2_5_ma14",
    "sin_doy",
    "cos_doy",
]

def predict_models(df_feat):
    """
    df_feat = output of build_features()
    Returns: pred_weather, pred_full
    """

    # --- Load models ---
    model1_path = os.path.join("data", "models", "model1_xgb.json")
    model2_path = os.path.join("data", "models", "model2_xgb.json")

    m1 = xgb.XGBRegressor()
    m1.load_model(model1_path)

    m2 = xgb.XGBRegressor()
    m2.load_model(model2_path)

    # --- Get today's latest row ---
    x_today = df_feat.iloc[-1]

    # --- Extract X vectors for both models ---
    X1 = x_today[model1_features].values.reshape(1, -1)
    X2 = x_today[model2_features].values.reshape(1, -1)

    # --- Predict ---
    pred_weather = float(m1.predict(X1)[0])
    pred_full    = float(m2.predict(X2)[0])

    return pred_weather, pred_full
