#%%
from connection import db_params
from api import data
import psycopg2
from psycopg2.extras import Json
from datetime import datetime

#%% Creating connection

conn = psycopg2.connect(**db_params)
cur = conn.cursor()
print(f"Conectado ao banco")

#%% Creating bronze table 

schema = """
    CREATE SCHEMA IF NOT EXISTS crypto_data;
    """

bronze_table = """
    CREATE TABLE IF NOT EXISTS crypto_data.bronze_table(
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL,
        ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""" 
cur.execute(bronze_table)
conn.commit()
print("Tabela criada")

#%% Ingestão de dados puros na tabela bronze (raw)

def run_ingestion():
    
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        print(f"Conectado ao banco")
        
        query = "INSERT INTO crypto_data.bronze_table (data) VALUES (%s)"
        start_time = datetime.now()
        for coin_entry in data:
            cur.execute(query, [Json(coin_entry)])
        conn.commit()
        print('Ingestão concluída')

    except Exception as e:
        print(f"Falha na ingestão: {e}")
        if conn:
            conn.rollback()
    
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Conexão Encerrada")
            
            #logs
            concluded = datetime.now()
            print(f"--- Relatório de Ingestão ---")
            print(f"Início: {start_time.strftime('%H:%M:%S')}")
            print(f"Conclusão: {concluded.strftime('%H:%M:%S')}")
            print(f"Duração: {concluded - start_time}")
    
#%%
run_ingestion()