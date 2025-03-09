# %%
import os
from datetime import datetime 
import pandas as pd

BRONZE_LAYER_PATH = r"..\..\..\data\bronze"
SILVER_LAYER_PATH = r"..\..\..\data\silver"
GOLD_LAYER_PATH = r"..\..\..\data\gold"

def load(file_path, file_name):
    return pd.read_parquet(os.path.join(file_path, file_name), index = 'date')	

# %%
write_options = {
    'compression': 'gzip',
    'engine': 'fastparquet'
}

def get_clean_prices(prices: pd.DataFrame, components: pd.DataFrame) -> pd.DataFrame:
    # mask the prices dataframe to only include the active tickers for each date
    return prices[components]

# prices = load(
#     file_path=BRONZE_LAYER_PATH, 
#     file_name='sp500_prx.parquet'
#     )

## add new data to the prices dataframe


## mask the prices dataframe to only include the active tickers for each date
# components = load(
#     file_path=SILVER_LAYER_PATH, 
#     file_name='sp500_components.parquet'
#     )
# %%
def main():
    # load prices previously stored as a parquet file
    prices = load(
        file_path=BRONZE_LAYER_PATH, 
        file_name='sp500_prx.parquet'
        )
    
    # load components previously stored as a parquet file
    components = load(
        file_path=SILVER_LAYER_PATH, 
        file_name='sp500_components.parquet'
        )
    
    # mask the prices dataframe to only include the active tickers for each date
    clean_prices = get_clean_prices(prices, components)
    
    # write to parquet file
    file_name = "sp500_prx.parquet"
    clean_prices.to_parquet(
        os.path.join(GOLD_LAYER_PATH, file_name),
        **write_options
        )
    
# %%
if __name__ == '__main__':
    main()