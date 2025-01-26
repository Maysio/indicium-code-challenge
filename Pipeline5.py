import pandas as pd
from sqlalchemy import create_engine

# Load the CSV file into a DataFrame
csv_file_path = 'path/to/your/order_details.csv'
order_details_df = pd.read_csv(csv_file_path)

# Connect to PostgreSQL database
engine = create_engine('postgresql://username:password@localhost:5432/northwind')
connection = engine.connect()

# Define the schema for the order_details table and create it
create_table_query = """
CREATE TABLE IF NOT EXISTS order_details (
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL,
    discount DECIMAL
);
"""
connection.execute(create_table_query)

# Insert data into the order_details table
order_details_df.to_sql('order_details', con=engine, index=False, if_exists='append')

# Close the connection
connection.close()
