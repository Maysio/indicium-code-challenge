from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import psycopg2

# Função para extrair dados do PostgreSQL
def extract_postgres():
    conn = psycopg2.connect(dbname='northwind', user='northwind_user', password='thewindisblowing', host='localhost', port='5432')
    query = "SELECT * FROM customer_customer_demo, customer_demographics, employee_territories, orders, customers, products, shippers, suppliers, territories, us_states, categories, region, employees"
    df = pd.read_sql(query, conn)
    df.to_csv('postgres_data.csv', index=False)
    conn.close()

# Função para extrair dados do CSV
def extract_csv():
    df = pd.read_csv('data/order_details.csv')
    df.to_csv('csv_data.csv', index=False)

# Função para combinar dados e salvar em JSON
def transform_and_load():
    df_postgres = pd.read_csv('/path/to/save/postgres_data.csv')
    df_csv = pd.read_csv('/path/to/save/csv_data.csv')
    df_combined = pd.concat([df_postgres, df_csv], ignore_index=True)
    df_combined.to_json('/path/to/save/combined_data.json')
    df_combined.to_csv('/path/to/save/combined_data.csv', index=False)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Um pipeline de dados simples',
    schedule_interval='@daily',
)

t1 = PythonOperator(
    task_id='extract_postgres',
    python_callable=extract_postgres,
    dag=dag,
)

t2 = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    dag=dag,
)

t3 = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_and_load,
    dag=dag,
)

t1 >> t2 >> t3
