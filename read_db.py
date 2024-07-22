import pandas as pd
import psycopg2


conn = psycopg2.connect(
    host="localhost",
    database="hyperleda",
    user="hyperleda",
    password="password",
    port="6432"
)

query = "select * from m000 limit 10"
df = pd.read_sql_query(query, conn)

conn.close()

print(df)