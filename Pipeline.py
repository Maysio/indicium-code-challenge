import pandas as pd

# Load CSV data
csv_data = pd.read_csv('./data/order_details.csv')

import psycopg2
import pandas as pd

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="northwind",
    user="northwind_user",
    password="thewindisblowing"
)

# Query to extract data
query = "SELECT * FROM customer_customer_demo; customer_demographics; employee_territories; orders; customers; products; shippers; suppliers; territories; us_states; categories; region; employees;"
db_data = pd.read_sql(query, conn)

# Close the connection
conn.close()

csv_data.to_csv('./data/csv/order_details.csv', index=False)

db_data.to_csv('./db_data.csv', index=False)

# Re-establish connection
conn = psycopg2.connect(
    host="localhost",
    database="northwind",
    user="northwind_user",
    password="thewindisblowing"
)

# Load data into PostgreSQL
csv_data.to_sql(northwind, conn, if_exists='replace', index=False)
conn.close()

# Re-establish connection
conn = psycopg2.connect(
    host="localhost",
    database="northwind",
    user="northwind_user",
    password="thewindisblowing"
)

# Load data into PostgreSQL
db_data.to_sql(customer_customer_demo; customer_demographics; employee_territories; orders; customers; products; shippers; suppliers; territories; us_states; categories; region; employees;"
, conn, if_exists='replace', index=False)
conn.close()
