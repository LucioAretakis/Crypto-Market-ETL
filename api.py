#%%
import requests
import pandas as pd
#%% Extraindo dados 

headers = {
    'Accept': 'application/json',
    'User-Agent': 'crypto-etl-pipeline/1.0'

}

# Getting coins/markets endpoint
url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin&names=Bitcoin&symbols=btc&category=layer-1&price_change_percentage=1h'
params = {
    'vs_currency':'usd',
    'order': 'market_cap_desc',
    'per_page': 250,
    'page': 1
}
  # Getting data from enpoint
response = requests.get(url, params=params, headers=headers, timeout=10)

# Turning into a Json Structured return
data = response.json()
