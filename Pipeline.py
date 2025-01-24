import pandas as pd
import psycopg2
from psycopg2 import sql

# Conectar ao banco de dados PostgreSQL (fonte)
conn_source = psycopg2.connect(
    dbname='northwind',
    user='northwind_user',
    password='thewindisblowing',
    host='localhost',
    port='5432'
)
cursor_source = conn_source.cursor()

# Extrair dados do banco de dados PostgreSQL (fonte)
query_source = "SELECT * FROM customer_customer_demo"
cursor_source.execute(query_source)
rows = cursor_source.fetchall()

# Criar um DataFrame a partir dos dados extraídos
columns = [desc[0] for desc in cursor_source.description]
df_source = pd.DataFrame(rows, columns=columns)

# Fechar a conexão com o banco de dados PostgreSQL (fonte)
cursor_source.close()
conn_source.close()

# Ler dados do arquivo CSV
df_csv = pd.read_csv('caminho_do_arquivo.csv')

# Combinar os dados extraídos
df_combined = pd.concat([df_source, df_csv], ignore_index=True)

# Armazenar os dados combinados em disco local
df_combined.to_csv('dados_combinados.csv', index=False)

# Conectar ao banco de dados PostgreSQL (destino)
conn_dest = psycopg2.connect(
    dbname='nome_do_banco_de_dados_destino',
    user='usuario_destino',
    password='senha_destino',
    host='host_destino',
    port='porta_destino'
)
cursor_dest = conn_dest.cursor()

# Carregar os dados armazenados no disco local para o banco de dados PostgreSQL (destino)
df_combined = pd.read_csv('dados_combinados.csv')
for i, row in df_combined.iterrows():
    cursor_dest.execute(
        sql.SQL("INSERT INTO tabela_destino (coluna1, coluna2, ...) VALUES (%s, %s, ...)"),
        tuple(row)
    )

# Confirmar a transação
conn_dest.commit()

# Fechar a conexão com o banco de dados PostgreSQL (destino)
cursor_dest.close()
conn_dest.close()
