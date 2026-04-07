#%%
import pandas as pd
import numpy as np
import yfinance as yf
import logging
from connection import db_params
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# Configuração do Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Iniciando o processo de ETL para a camada Gold...")

#%% Creating connection parameters and function 

user = db_params['user']
password = quote_plus(db_params['password'])
host = db_params['host']
port = db_params['port']
db = db_params['database']

def get_engine():
    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_string)

#%% Query to get last processed batch on silver_table for every coin_id

query = """
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY processed_at DESC) as posicao
    FROM crypto_data.silver_table
) sub
WHERE posicao = 1;
"""

#%% Reading data from silver table

engine = get_engine()
logger.info("Tentando conectar ao banco de dados para extração da camada Silver...")

try:
    with engine.connect() as conn:
        logger.info("Conexão com o banco de dados estabelecida com sucesso.")
    
    df = pd.read_sql(query, engine)
    logger.info(f"Tabela Silver lida com sucesso! {len(df)} registros extraídos.")
except Exception as e:
    logger.error(f"Erro ao extrair dados da camada Silver: {e}")
    raise # É boa prática levantar o erro para o script parar se a extração falhar
 
#%% Applying business rules

# 1. Changing currency from USD to BRL
logger.info("Buscando cotação atual do Dólar (USD para BRL) via yFinance...")

ticker_simbolo = "USDBRL=X" 
ticker = yf.Ticker(ticker_simbolo)
dados = ticker.history(period="1d")

if not dados.empty:
    cotacao = dados['Close'].iloc[-1]
    logger.info(f"Cotação Atual ({ticker_simbolo}) obtida com sucesso: R$ {cotacao:.4f}")
else:
    logger.error(f"Erro: Não foi possível encontrar dados para o ticker {ticker_simbolo}")

#%% Inserting prices in BRL
logger.info("Iniciando conversão de valores das colunas financeiras para BRL...")

list_BRL = ['current_price(BRL)', 'high_24h(BRL)', 'low_24h(BRL)', 'alltime_high(BRL)', 'alltime_low(BRL)']
list_USD = ['current_price', 'high_24h', 'low_24h', 'alltime_high', 'alltime_low']
pos = [df.columns.get_loc(col) for col in list_USD]

df[list_BRL] = df.iloc[:, pos] * cotacao
logger.info("Conversão para BRL concluída em todas as colunas de preço.")

#%% Market Cap Ranking using numpy
logger.info("Aplicando regra de negócio: Categorização de Market Tier (Large/Medium/Low Cap)...")

condition = [
    (df['market_cap'] >= 100_000_000_000),
    (df['market_cap'] >= 50_000_000_000),
    (df['market_cap'] < 50_000_000_000)
]

cap = ['Large Cap', 'Medium Cap', 'Low Cap']

# Creating Market Tier column
df['market_tier'] = np.select(condition, cap, default='Uknown')
logger.info("Categorização de Market Tier aplicada com sucesso.")

#%% Market Cap position
logger.info("Calculando o ranking das moedas baseado no Market Cap (Market Rank)...")

df['market_rank'] = df['market_cap'].rank(method='min', ascending=False)
df['market_rank'] = df['market_rank'].astype(int)
logger.info("Ranking calculado e ordenado com sucesso.")

#%% Performance -- there should not be price_change values < 0
logger.info("Aplicando máscara para remover valores negativos nas colunas de performance...")

colunas_performance = ['price_change_1h', 'price_change_24h']
df[colunas_performance] = df[colunas_performance].mask(df[colunas_performance] < 0, 0)
logger.info("Tratamento de valores negativos concluído.")

#%% Create gold_table and commit df
logger.info("Verificando e criando a estrutura da tabela Gold no banco de dados (DDL)...")

create_table = """
CREATE TABLE IF NOT EXISTS crypto_data.gold_table(
    -- Identificadores e Informações Básicas
    coin_id VARCHAR(100) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    coin_name VARCHAR(100) NOT NULL,
    market_rank INT,
    market_tier VARCHAR(50),

    -- Preços e Variações (Em USD ou Moeda Base)
    current_price DECIMAL(20, 10),
    price_change_1h DECIMAL(10, 4),
    price_change_24h DECIMAL(10, 4),
    market_cap DECIMAL(25, 2),
    high_24h DECIMAL(20, 10),
    low_24h DECIMAL(20, 10),
    alltime_high DECIMAL(20, 10),
    alltime_low DECIMAL(20, 10),

    -- Valores Convertidos para Real (BRL)
    current_price_brl DECIMAL(20, 10),
    high_24h_brl DECIMAL(20, 10),
    low_24h_brl DECIMAL(20, 10),
    alltime_high_brl DECIMAL(20, 10),
    alltime_low_brl DECIMAL(20, 10),

    -- Timestamps e Controle
    last_updated TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

#%% Truncate gold table before inserting data
logger.info("Executando TRUNCATE na tabela Gold para preparar a inserção do novo lote...")

truncate_table = """
TRUNCATE TABLE crypto_data.gold_table;
"""

#%% Inserting values 
insert_gold_query = """
    INSERT INTO crypto_data.gold_table (
        coin_id, symbol, coin_name, market_rank, market_tier,
        current_price, price_change_1h, price_change_24h, market_cap, 
        high_24h, low_24h, alltime_high, alltime_low,
        current_price_brl, high_24h_brl, low_24h_brl, 
        alltime_high_brl, alltime_low_brl,
        last_updated
    ) VALUES (
        :coin_id, :symbol, :coin_name, :market_rank, :market_tier,
        :current_price, :price_change_1h, :price_change_24h, :market_cap, 
        :high_24h, :low_24h, :alltime_high, :alltime_low,
        :current_price_brl, :high_24h_brl, :low_24h_brl, 
        :alltime_high_brl, :alltime_low_brl,
        :last_updated
    )
    ON CONFLICT (coin_id) 
    DO UPDATE SET 
        market_rank = EXCLUDED.market_rank,
        market_tier = EXCLUDED.market_tier,
        current_price = EXCLUDED.current_price,
        current_price_brl = EXCLUDED.current_price_brl,
        last_updated = EXCLUDED.last_updated;
"""

# Proccess execution 
try:
    # 3.1 Create table (if it doesnt exist)
    with engine.begin() as conn:
        conn.execute(text(create_table))
        logger.info("Tabela Gold verificada/criada com sucesso.")
        
    # 3.2 Truncate table
    with engine.begin() as conn:
        conn.execute(text(truncate_table))
        logger.info("Tabela Gold limpa com sucesso (TRUNCATE).")

    # 3.3 Preparing data
    logger.info("Renomeando colunas e formatando DataFrame para inserção...")
    df_gold_ready = df.rename(columns={
        'current_price(BRL)': 'current_price_brl',
        'high_24h(BRL)': 'high_24h_brl',
        'low_24h(BRL)': 'low_24h_brl',
        'alltime_high(BRL)': 'alltime_high_brl',
        'alltime_low(BRL)': 'alltime_low_brl'
    })

    # Convert list to dictionaries 
    data_to_insert = df_gold_ready.replace({np.nan: None}).to_dict(orient='records')
    logger.info("Dados formatados como dicionários para o SQLAlchemy.")

    # 3.4 Batch Insert
    logger.info(f"Iniciando inserção no banco: {len(data_to_insert)} registros...")
    with engine.begin() as conn:
        conn.execute(text(insert_gold_query), data_to_insert)
        logger.info(f"Sucesso: {len(data_to_insert)} registros inseridos/atualizados na Gold.")

    logger.info("Processo da camada Gold finalizado com sucesso!")

except Exception as e:
    logger.error(f"Erro crítico na operação de banco de dados durante a carga na Gold: {e}")