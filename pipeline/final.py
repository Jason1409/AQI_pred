import pandas as pd
import psycopg2

# import your functions
from DB.aqi_db import fetch_last_n_days, update_predictions
from Features.build_features import build_features
from traffic.traffic_calc import add_proxy_traffic_factor_from_no2_co, apply_traffic_adjustment
from pipeline.predict_models import predict_models
from pipeline.aqi import aqi_india_pm25


def run_daily_pipeline(conn, days: int = 30, traffic_factor: float = 1.0):
    """
    Full PM2.5 + AQI prediction pipeline.

    Steps:
        1. Fetch last N days raw data from DB
        2. Build NO2+CO traffic proxy
        3. Feature engineering
        4. Run both XGBoost models
        5. Compute traffic component
        6. Apply traffic adjustment
        7. Compute AQI
        8. Update DB for the latest date
    """

    # ---------- 1. Fetch RAW ----------
    df_raw = fetch_last_n_days(conn, days)
    df_raw["date"] = pd.to_datetime(df_raw["date"])
    df_raw = df_raw.sort_values("date")


    if df_raw.empty:
        raise ValueError("No data fetched from DB. Cannot run pipeline.")

    df_raw = df_raw.sort_values("date")

    # ---------- 2. Traffic proxy from NO2 + CO ----------
    df_raw = add_proxy_traffic_factor_from_no2_co(df_raw)
    # df_raw now has: traffic_factor_proxy

    # ---------- 3. Feature engineering ----------
    df_feat = build_features(df_raw)


    # ---------- 5. Model Predictions ----------
    pmw, pmf = predict_models(df_feat)
    df_feat["pm25_pred_weather"] = pmw
    df_feat["pm25_pred_full"] = pmf

    # ---------- 6. Traffic Component ----------
    df_feat["traffic_component"] = df_feat["pm25_pred_full"] - df_feat["pm25_pred_weather"]

    # ---------- 7. Apply traffic adjustment ----------
    df_feat = apply_traffic_adjustment(
        df_feat,
        traffic_factor=traffic_factor,               # global scenario scaling
        per_row_fac_col="traffic_factor_proxy"       # dynamic NO2+CO proxy
    )

    # ---------- 8. Compute AQI for the latest day ----------
    latest = df_feat.sort_values("date").iloc[-1:].copy()
    latest["aqi"] = latest["pm25_final_adjusted"].apply(aqi_india_pm25)

    # ---------- 9. Update DB ----------
    update_predictions(conn, latest)

    return latest
