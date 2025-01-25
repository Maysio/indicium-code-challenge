import psycopg2
import pandas as pd
from datetime import datetime

def extract_csv(file_path):
    return pd.read_csv(file_path)

def extract_postgres(conn_params, query):
    conn = psycopg2.connect(**conn_params)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def transform_data(csv_data, db_data):
    merged_data = pd.concat([csv_data, db_data], ignore_index=True)
    return merged_data

def load_to_disk(data, file_path):
    data.to_csv(file_path, index=False)

def load_from_disk(file_path):
    return pd.read_csv(file_path)

def load_to_postgres(data, conn_params, table_name):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    # Assuming the table exists and has the appropriate columns
    for index, row in data.iterrows():
        cursor.execute(
            f"INSERT INTO {table_name} (column1, column2, ...) VALUES (%s, %s, ...)", 
            tuple(row)
        )
    
    conn.commit()
    cursor.close()
    conn.close()

# PostgreSQL connection parameters
postgres_conn_params = {
    'dbname': 'your_dbname',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}

csv_file_path = 'path/to/source_data.csv'
local_save_path = f'path/to/save/merged_data_{datetime.now().strftime("%Y%m%d")}.csv'
postgres_query = "SELECT * FROM your_table_name"

# Running the pipeline steps independently
if __name__ == "__main__":
    # Step 1: Extract Data from CSV
    csv_data = extract_csv(csv_file_path)
    
    # Step 2: Extract Data from PostgreSQL
    db_data = extract_postgres(postgres_conn_params, postgres_query)
    
    # Step 3: Transform Data
    merged_data = transform_data(csv_data, db_data)
    
    # Step 4: Save Data to Local Disk
    load_to_disk(merged_data, local_save_path)
    
    # Step 5: Load Data from Local Disk
    loaded_data = load_from_disk(local_save_path)
    
    # Step 6: Insert Data into PostgreSQL
    load_to_postgres(loaded_data, postgres_conn_params, 'your_table_name')
