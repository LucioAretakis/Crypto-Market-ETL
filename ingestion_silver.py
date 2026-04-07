#%%
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from connection import db_params
from api import data

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ingestion_silver.log"), # Saves logs to a file
        logging.StreamHandler() # Displays logs in the console
    ]
)
logger = logging.getLogger(__name__)

#%% Database Connection

try:
    logger.info("Iniciando conexão com o banco")
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    logger.info("Conexão bem-sucedida.")
except Exception as e:
    logger.error(f"Falha ao conectar: {e}")
    raise

#%% Creating DataFrame and Mapping Columns

try:
    logger.info("Iniciando normalização")
    df = pd.json_normalize(data)

    # Defining the source columns from API
    columns_df = [
        'id', 'symbol', 'name', 'current_price', 
        'price_change_percentage_1h_in_currency', 
        'price_change_percentage_24h', 'market_cap', 
        'high_24h', 'low_24h', 'ath', 'atl', 'last_updated'
    ]

    # Reindexing to ensure strict column structure and order
    df = df.reindex(columns=columns_df)

    # Renaming columns to match the Silver Table schema
    df.columns = [
        'coin_id', 'symbol', 'coin_name', 'current_price', 'price_change_1h', 
        'price_change_24h', 'market_cap', 'high_24h', 'low_24h', 
        'alltime_high', 'alltime_low', 'last_updated'
    ]
    logger.info(f"Mapping completed. DataFrame shape: {df.shape}")

except Exception as e:
    logger.error(f"Error during data mapping: {e}")
    conn.close()
    raise

#%% Data Transformation and Cleaning

try:
    logger.info("Iniciando transformação e limpeza dos dados")
    
    # Converting string to datetime object
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    
    # Standardizing symbols to uppercase
    df['symbol'] = df['symbol'].str.upper()
    
    # Converting NaN values to None (SQL NULL compatible)
    df = df.replace({np.nan: None})
    
    logger.info("Transformação finalizada")
except Exception as e:
    logger.error(f"Erro na transformação: {e}")
    conn.close()
    raise

#%% Silver Table Creation

silver_table_query = """
    CREATE TABLE IF NOT EXISTS crypto_data.silver_table(
        coin_id VARCHAR(50),
        symbol VARCHAR(10),
        coin_name VARCHAR(30),
        current_price DECIMAL(18,8),
        price_change_1h DECIMAL(18,8),
        price_change_24h DECIMAL(18,8),
        market_cap BIGINT,
        high_24h DECIMAL(18,8),
        low_24h DECIMAL(18,8),
        alltime_high DECIMAL(18,8),
        alltime_low DECIMAL(18,8),
        last_updated TIMESTAMP,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (coin_id, last_updated)  
    );
"""

try:
    logger.info("Verificando existência da tabela prata")
    cur.execute(silver_table_query)
    conn.commit()
    logger.info("Tabela conferida/criada")
except Exception as e:
    logger.error(f"Erro ao criar tabela: {e}")
    conn.rollback()
    raise

#%% Inserting Data into Silver Table (Batch Ingestion)

insert_query = """
    INSERT INTO crypto_data.silver_table(
        coin_id, symbol, coin_name, current_price, price_change_1h, 
        price_change_24h, market_cap, high_24h, low_24h, 
        alltime_high, alltime_low, last_updated
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (coin_id, last_updated) DO NOTHING; 
"""

try:
    # Converting DataFrame rows into a list of tuples for psycopg2
    data_tuples = [tuple(x) for x in df.values]
    
    logger.info(f"Iniciando ingestão de {len(data_tuples)} registros...")
    execute_batch(cur, insert_query, data_tuples)
    conn.commit()
    logger.info("Ingestão da camada prata bem-sucedida.")

except Exception as e:
    logger.error(f"Erro durante a ingestão: {e}")
    conn.rollback()
    logger.warning("Rollback devido à erro")
finally:
    # Closing resources
    cur.close()
    conn.close()
    logger.info("Conexão fechada.")
