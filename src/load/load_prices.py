
###
# Code to load prices data from the bronze layer
# and save it to the bronze layer in parquet format
###

# %%
import os
from datetime import datetime 
import pandas as pd

BRONZE_LAYER_PATH = r"..\..\data\bronze"

def load(file_path, file_name):
    return pd.read_csv(os.path.join(file_path, file_name), index_col="date", parse_dates=['date'])

# %%
write_options = {
    'compression': 'gzip',
    'engine': 'fastparquet'
}

def main():

    # load prices previously stored as a csv file
    prices = load(
        file_path=BRONZE_LAYER_PATH, 
        file_name='sp500_prx_20250308.csv.gz'
        ).reset_index()
    
    # write to parquet file
    file_name = f'sp500_prx_{datetime.now().strftime("%Y%m%d")}.parquet'
    prices.to_parquet(
        os.path.join(BRONZE_LAYER_PATH, file_name),
        **write_options
        )

# %%
if __name__ == '__main__':
    main()