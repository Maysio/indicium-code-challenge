import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Extract CSV data
def extract_csv_data(file_path, output_dir):
    csv_data = pd.read_csv(file_path)
    today = datetime.today().strftime('%Y-%m-%d')
    output_path = f'{output_dir}/csv/{today}/order_details.csv'
    csv_data.to_csv(output_path, index=False)
    print(f"CSV data extracted and saved locally at {output_path}.")

# Extract data from the database
def extract_db_data(conn_string, query, output_dir, table_name):
    engine = create_engine(conn_string)
    db_data = pd.read_sql(query, engine)
    today = datetime.today().strftime('%Y-%m-%d')
    output_path = f'{output_dir}/postgres/{table_name}/{today}/{table_name}.csv'
    db_data.to_csv(output_path, index=False)
    print(f"Database data from {table_name} extracted and saved locally at {output_path}.")

# Load CSV data into the database
def load_csv_to_db(csv_path, conn_string, table_name):
    csv_data = pd.read_csv(csv_path)
    engine = create_engine(conn_string)
    csv_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"CSV data from {csv_path} loaded into PostgreSQL table {table_name}.")

# Load database data from CSV into the database
def load_db_data_to_db(csv_path, conn_string, table_name):
    db_data = pd.read_csv(csv_path)
    engine = create_engine(conn_string)
    db_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Database data from {csv_path} loaded into PostgreSQL table {table_name}.")

# Connection string
conn_string = 'postgresql://northwind_user:thewindisblowing@localhost/northwind'

# Sample queries and table names
csv_query = "SELECT * FROM orders"
db_query = "SELECT * FROM customer_customer_demo"

# Run the steps
extract_csv_data('./data/order_details.csv', '/data')
extract_db_data(conn_string, db_query, '/data', 'customer_customer_demo')
load_csv_to_db('/data/csv/2024-01-02/order_details.csv', conn_string, 'order_details')
load_db_data_to_db('./data/db_data.csv', conn_string, 'customer_customer_demo')
