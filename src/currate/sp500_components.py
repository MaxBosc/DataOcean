
# ------------------------------------------------------------
# Project: Currate tickers availability in the sp500
# File: sp500_components.py
# Description: Code to load S&P 500 components data from the bronze layer
# and save it to the silver layer in parquet format

# ------------------------------------------------------------

# %%
import os
from datetime import datetime
import pandas as pd


BRONZE_LAYER_PATH = r"..\..\data\bronze"
SILVER_LAYER_PATH = r"..\..\data\silver"

# %%
def load(file_path, file_name):
    return pd.read_csv(
        os.path.join(file_path, file_name), index_col="date", parse_dates=['date'])

def space_components(components) -> pd.DataFrame:
    return components.tickers.str.split(',', expand=True)

def stack_tickers(df) -> pd.DataFrame:
    df = (
        df.stack(dropna=True)
        .droplevel(1)
        .reset_index()
        .set_index(["date", 0])
        )
    return df

def add_boolean_presence(df) -> pd.DataFrame:
    df.index.names = ["date", "ticker"]
    df.loc[:, 'available'] = True
    return df

def fill_to_date(df, end_date: datetime) -> pd.DataFrame:
    date_range = pd.date_range(df.index.get_level_values(0).min(), end_date)
    df = df.reindex(date_range, method='ffill')
    return df


# %%
def main():

    components = load(
        file_path=BRONZE_LAYER_PATH, 
        file_name='S&P 500 Historical Components & Changes(12-10-2024).csv'
        )
    
    # transform to have the tickers as columns with a boolean presence
    df = (
        components
        .pipe(space_components)
        .pipe(stack_tickers)
        .pipe(add_boolean_presence)
        .unstack(level=1, fill_value=False)
        .droplevel(0, axis=1)
        .sort_index()
        .pipe(fill_to_date, end_date = datetime.today())
        )

    # write to parquet file
    file_name = f'sp500_components_{datetime.now().strftime("%Y%m%d")}.parquet'
    df.to_parquet(
        os.path.join(SILVER_LAYER_PATH, file_name),
        engine='fastparquet'
        )

# %%
if __name__ == '__main__':
    main()