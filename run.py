import psycopg2
from pipeline.final import run_daily_pipeline

conn = psycopg2.connect(
    dbname="AQI",
    user="postgres",
    password="Jason003",
    host="localhost",
    port=5432
)

result = run_daily_pipeline(conn)
print(result)
