import pandas as pd
from datetime import datetime

def extract_csv_data(file_path, output_dir):
    csv_data = pd.read_csv(file_path)
    today = datetime.today().strftime('%Y-%m-%d')
    output_path = f'{output_dir}/csv/{today}/order_details.csv'
    csv_data.to_csv('./data/order_details.csv', index=False)
    print(f"CSV data extracted and saved locally at {output_path}.")

# Run the step
extract_csv_data('./data/order_details.csv', '/data')

import psycopg2
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

def extract_db_data(conn_params, query, output_dir, table_name):
#    conn = psycopg2.connect(**conn_params)
    engine = create_engine(conn_string)
#    db_data = pd.read_sql(query, conn)
    db_data = pd.read_sql(query, engine)
    today = datetime.today().strftime('%Y-%m-%d')
    output_path = f'{output_dir}/postgres/{table_name}/{today}/{table_name}.csv'
    db_data.to_csv(output_path, index=False)
    conn.close()
    print(f"Database data from {table_name} extracted and saved locally at {output_path}.")
    
# Connection parameters and query
conn_params = {
    'host': 'localhost',
    'database': 'northwind',
    'user': 'northwind_user',
    'password': 'thewindisblowing'
}
conn_string = 'postgresql://northwind_user:thewindisblowing@localhost/northwind'

query = "SELECT * FROM customer_customer_demo, customer_demographics, employee_territories, orders, customers, products, shippers, suppliers, territories, us_states, categories, region, employees"
table_name = 'customer_customer_demo, customer_demographics, employee_territories, orders, customers, products, shippers, suppliers, territories, us_states, categories, region, employees'
# Run the step
extract_db_data(conn_params, query, '/data', table_name)

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def load_csv_to_db(csv_path, conn_string, table_name):
    csv_data = pd.read_csv(csv_path)
    engine = create_engine(conn_string)
    csv_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"CSV data from {csv_path} loaded into PostgreSQL table {table_name}.")

# Connection string
conn_string = 'postgresql://northwind_user:thewindisblowing@localhost/northwind'

# Run the step
load_csv_to_db('/data/csv/2024-01-02/order_details.csv', conn_string, 'order_details')

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def load_db_data_to_db(csv_path, conn_string, table_name):
    db_data = pd.read_csv(csv_path)
    engine = create_engine(conn_string)
    db_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Database data from {csv_path} loaded into PostgreSQL table {table_name}.")
# Connection string
conn_string = 'postgresql://northwind_user:thewindisblowing@localhost/northwind'

# Run the step
load_db_data_to_db('./data/db_data.csv', conn_string, 'customer_customer_demo, customer_demographics, employee_territories, orders, customers, products, shippers, suppliers, territories, us_states, categories, region, employees')
load_db_data_to_db('/data/postgres/{table}/{date}/{table}.csv', conn_string, 'your_table')
