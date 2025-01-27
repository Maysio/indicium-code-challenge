import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Connection string
conn_string = 'postgresql://northwind_user:thewindisblowing@localhost/northwind'

# Load CSV data into the PostgreSQL database
def load_db_data_to_db(csv_path, conn_string, table_name):
    db_data = pd.read_csv(csv_path)
    engine = create_engine(conn_string)
    db_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Database data from {csv_path} loaded into PostgreSQL table {table_name}.")

# Load the data
load_db_data_to_db('./data/db_data.csv', conn_string, 'orders')
