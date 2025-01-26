import psycopg2
import pandas as pd
from datetime import datetime

def extract_csv(file_path):
    return pd.read_csv(file_path)

def extract_postgres(conn_params):
    conn = psycopg2.connect(**conn_params)
    query = "SELECT * FROM your_table_name"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def transform_data(csv_data, db_data):
    # Example transformation: concatenate data
    merged_data = pd.concat([csv_data, db_data], ignore_index=True)
    return merged_data

def load_to_disk(data, file_path):
    data.to_csv(file_path, index=False)

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

def main():
    csv_file_path = 'path/to/source_data.csv'
    local_save_path = f'path/to/save/merged_data_{datetime.now().strftime("%Y%m%d")}.csv'
    
    # PostgreSQL connection parameters
    postgres_conn_params = {
        'dbname': 'your_dbname',
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Extraction
    csv_data = extract_csv(csv_file_path)
    db_data = extract_postgres(postgres_conn_params)
    
    # Transformation
    merged_data = transform_data(csv_data, db_data)
    
    # Load to local disk
    load_to_disk(merged_data, local_save_path)
    
    # Load to PostgreSQL
    load_to_postgres(merged_data, postgres_conn_params, 'your_table_name')

if __name__ == "__main__":
    main()
