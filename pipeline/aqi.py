def aqi_india_pm25(pm25):
    """
    Compute Indian CPCB AQI using ONLY PM2.5 (24h average).
    Returns integer AQI or None.
    """

    if pm25 is None:
        return None

    try:
        C = float(pm25)
    except:
        return None

    if C < 0:
        return None

    # CPCB NAQI breakpoints for PM2.5
    breakpoints = [
        (0,   30,   0,   50),     # Good
        (31,  60,   51,  100),    # Satisfactory
        (61,  90,   101, 200),    # Moderately Polluted
        (91,  120,  201, 300),    # Poor
        (121, 250,  301, 400),    # Very Poor
        (251, 800,  401, 500),    # Severe
    ]

    for BP_lo, BP_hi, I_lo, I_hi in breakpoints:
        if BP_lo <= C <= BP_hi:
            # Linear interpolation
            I = (I_hi - I_lo) / (BP_hi - BP_lo) * (C - BP_lo) + I_lo
            return int(round(I))

    # If above range (extreme pollution)
    BP_lo, BP_hi, I_lo, I_hi = breakpoints[-1]
    I = (I_hi - I_lo) / (BP_hi - BP_lo) * (min(C, BP_hi) - BP_lo) + I_lo
    return int(round(I))
