import pandas as pd

def extract_csv_data(file_path):
    csv_data = pd.read_csv(file_path)
    csv_data.to_csv('./data/order_details.csv', index=False)
    print("CSV data extracted and saved locally.")


# Run the step
extract_csv_data('./data/order_details.csv')

conn = psycopg.connect(
    host="localhost",
    database="northwind",
    user="northwind_user",
    password="thewindisblowing"
)
import psycopg
import pandas as pd

def extract_db_data(conn_params, query, output_path):
    conn = psycopg.connect(**conn_params)
    db_data = pd.read_sql(query, conn)
    db_data.to_csv(output_path, index=False)
    conn.close()
    print("Database data extracted and saved locally.")

# Connection parameters and query
conn_params = {
    'host': 'localhost',
    'database': 'northwind',
    'user': 'northwind_user',
    'password': 'thewindisblowing'
}
query = "SELECT * FROM customer_customer_demo, customer_demographics, employee_territories, orders, customers, products, shippers, suppliers, territories, us_states, categories, region, employees"
output_path = './data/db_data.csv'

# Run the step
extract_db_data(conn_params, query, output_path)


import psycopg
import pandas as pd
from sqlalchemy import create_engine

def load_csv_to_db(csv_path, conn_string, table_name):
    csv_data = pd.read_csv(csv_path)
    engine = create_engine(conn_string)
    csv_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print("CSV data loaded into PostgreSQL database.")

# Connection string
conn_string = 'postgresql://northwind_user:thewindisblowing@localhost/northwind'

# Run the step
load_csv_to_db('./data/order_details.csv', conn_string, 'order_details')

import psycopg
import pandas as pd
from sqlalchemy import create_engine

def load_db_data_to_db(csv_path, conn_string, table_name):
    db_data = pd.read_csv(csv_path)
    engine = create_engine(conn_string)
    db_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Database data loaded into PostgreSQL database.")

# Connection string
conn_string = 'postgresql://northwind_user:thewindisblowing@localhost/northwind'

# Run the step
load_db_data_to_db('./data/db_data.csv', conn_string, 'customer_customer_demo, customer_demographics, employee_territories, orders, customers, products, shippers, suppliers, territories, us_states, categories, region, employees')
