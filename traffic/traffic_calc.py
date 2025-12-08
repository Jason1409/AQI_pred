import pandas as pd
import numpy as np

def apply_traffic_adjustment(
    df: pd.DataFrame,
    traffic_factor: float = 1.0,
    per_row_fac_col: str | None = None
) -> pd.DataFrame:
    """
    Apply final traffic adjustment:
        effective_factor = traffic_factor_proxy * traffic_factor
    """

    df = df.copy()

    if per_row_fac_col is not None:
        if per_row_fac_col not in df.columns:
            raise ValueError(f"Column '{per_row_fac_col}' not found in DataFrame.")
        
        # Combine per-row (proxy) AND global scenario factor
        tf = df[per_row_fac_col].astype(float) * float(traffic_factor)

    else:
        # No proxy provided → only global factor
        tf = float(traffic_factor)

    df["pm25_final_adjusted"] = (
        df["pm25_pred_weather"] + tf * df["traffic_component"]
    )

    df["pm25_final_adjusted"] = df["pm25_final_adjusted"].clip(lower=0)
    return df


def add_proxy_traffic_factor_from_no2_co(
    df: pd.DataFrame,
    no2_col: str = "no2",
    co_col: str = "co",
    window: int = 30,
    w_no2: float = 0.6,
    w_co: float = 0.4,
    min_factor: float = 0.4,
    max_factor: float = 1.6,
) -> pd.DataFrame:
    """
    Build a per-row traffic_factor using NO2 and CO as proxies.
    
    - window: rolling window (days) to estimate baseline levels.
    - w_no2, w_co: weights for NO2 and CO ratios.
    """
    df = df.copy()
    df = df.sort_values("date")

    # Rolling baselines (median to reduce outlier impact)
    df["no2_baseline"] = (
        df[no2_col]
        .rolling(window=window, min_periods=7)
        .median()
    )
    df["co_baseline"] = (
        df[co_col]
        .rolling(window=window, min_periods=7)
        .median()
    )

    # Avoid division by zero / NaN baselines
    df["no2_ratio"] = df[no2_col] / df["no2_baseline"].replace(0, np.nan)
    df["co_ratio"]  = df[co_col]  / df["co_baseline"].replace(0, np.nan)

    # If baseline is missing (beginning of series), fall back to 1.0 (no adjustment)
    df["no2_ratio"] = df["no2_ratio"].fillna(1.0)
    df["co_ratio"]  = df["co_ratio"].fillna(1.0)

    # Combined traffic index
    df["traffic_factor_proxy"] = w_no2 * df["no2_ratio"] + w_co * df["co_ratio"]

    # Center around 1 and avoid crazy extremes
    df["traffic_factor_proxy"] = df["traffic_factor_proxy"].clip(min_factor, max_factor)

    return df
