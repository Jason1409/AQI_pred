AQI Prediction with Weather + Traffic Proxy
This project predicts daily PM2.5 and AQI for Bangalore using weather data + pollutant signals from six major locations:
Silk Board
Koramangala
Indiranagar
Electronic City
Whitefield
Yeshwanthpur

The system automatically:
 Fetches yesterday’s pollutant & weather data
 Aggregates across all 6 locations
 Builds lag & moving-average features
 Predicts PM2.5 using two trained XGBoost models
 Separates weather impact vs traffic impact
 Computes final AQI (CPCB India standard)
 Saves the prediction directly to PostgreSQL
 
Tech Stack:
Python
XGBoost
Pandas
PostgreSQL
Open-Meteo APIs
Requests + caching + retry
Jupyter (training & visualization)
