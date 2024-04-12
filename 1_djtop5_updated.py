import pandas as pd
from datetime import date
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import requests
import yfinance as yf
import os 

OUTPUT_DIR = 'dags/data'
OUTPUT_FILE = 'top_5_ticker.csv'
# dtykey = '6AHG15XLB86AMCZX'
# epskey = '9ZLATZ1IC6GHUVAK'
# gdpkey = '6I2Y9UULKVW6XGX5'
# inflationkey = 'RYL3ULY9P5ZOJVUX'
# cpkey = 'L0S0FYJGSKZLWC8R' 
# rsikey = '63TMPZD2NJJ51WFA'
# key = 'REV6KO6RA6ADQDF6'
key = '6I2Y9UULKVW6XGX5'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3
}

def fetch_and_get_top_stocks():
    WIKI_URL = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
    response = requests.get(WIKI_URL)
    tables = pd.read_html(response.content)
    tickers = tables[1]['Symbol'].str.upper().tolist()  # Assuming the tickers are in the second table and converting to a list
    
    ticker_market_caps = []
    for ticker in tickers:
        market_cap = get_market_cap(ticker)
        if market_cap:
            ticker_market_caps.append((ticker, market_cap))  # Append ticker and market cap as tuple
    
    ticker_market_caps.sort(key=lambda x: x[1], reverse=True)  # Sort tickers by market cap in descending order
    
    top_tickers = [ticker[0] for ticker in ticker_market_caps[:5]]  # Extract top 5 tickers
    
    return pd.DataFrame({'Ticker': top_tickers})

def get_market_cap(ticker):
    stock = yf.Ticker(ticker)
    try:
        market_cap = stock.info['marketCap']
        return market_cap
    except KeyError:
        print(f"Market capitalization not available for {ticker}")
        return None
    
def save_tickers(ticker_df, directory=OUTPUT_DIR, filename=OUTPUT_FILE):
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    ticker_df.to_csv(filepath, index=False) 
    print(f'Tickers saved to {filepath}')

def dty():
    url = f'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval=daily&maturity=10year&apikey={key}' #changed to daily from default
    r = requests.get(url)
    data = r.json()
    time_series_data = data["data"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data)
    return df.head(1000)


def eps(ticker):
    url = f'https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey={key}'
    r = requests.get(url)
    data = r.json()
    time_series_data = data["quarterlyEarnings"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data)
    # Create a new column for the ticker symbol
    df['Ticker'] = ticker
    cols = list(df.columns)
    cols = ['Ticker'] + [col for col in cols if col != 'Ticker']
    df = df[cols]
    return df

def realgdppercapita():
    url = f'https://www.alphavantage.co/query?function=REAL_GDP_PER_CAPITA&apikey={key}'
    r = requests.get(url)
    data = r.json()
    time_series_data = data["data"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data)
    return df
    

def inflation():
    url = f'https://www.alphavantage.co/query?function=INFLATION&apikey={key}'
    r = requests.get(url)
    data = r.json()
    time_series_data = data["data"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data)
    return df

def cp(ticker):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={key}'
    # url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&outputsize=full&apikey=demo' # for demo data
    r = requests.get(url)
    data = r.json()
    # Extract the time series data from the response JSON
    time_series_data = data['Time Series (Daily)']
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    # Create a new column for the ticker symbol
    df['Ticker'] = ticker
    cols = list(df.columns)
    cols = ['Ticker'] + [col for col in cols if col != 'Ticker']
    df = df[cols]
    # Convert 'close' column to numeric
    #df['close'] = pd.to_numeric(df['4. close'])
    # Reverse the order of rows in the DataFrame
    #df = df[::-1].reset_index(drop=True)
    # Shift 'close' column by one row up to get tomorrow's close price
   # df['tomorrow_close'] = df['close'].shift(-1)
    #creating movement variable
    #df['movement'] = np.where(df['tomorrow_close'] > df['close'], 1, np.where(df['tomorrow_close'] < df['close'], -1, 0))
    
    return df.head(1000)

def rsi(ticker):
    url = f'https://www.alphavantage.co/query?function=RSI&symbol={ticker}&interval=daily&time_period=10&series_type=open&apikey={key}' #changed to daily
    r = requests.get(url)
    data = r.json()
    # Extract the time series data from the response JSON
    time_series_data = data['Technical Analysis: RSI']
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    # Create a new column for the ticker symbol
    df['Ticker'] = ticker
    cols = list(df.columns)
    cols = ['Ticker'] + [col for col in cols if col != 'Ticker']
    df = df[cols]
    return df.head(1000)

def sma(ticker):
    url = f'https://www.alphavantage.co/query?function=SMA&symbol=IBM&interval=daily&time_period=10&series_type=open&apikey={key}' #changed to daily
    r = requests.get(url)
    data = r.json()
    # Extract the time series data from the response JSON
    time_series_data = data["Technical Analysis: SMA"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    # Create a new column for the ticker symbol
    df['Ticker'] = ticker
    cols = list(df.columns)
    cols = ['Ticker'] + [col for col in cols if col != 'Ticker']
    df = df[cols]
    return df.head(1000)

def save_data_for_funcs_without_ticker():
    functions_to_run = [dty, realgdppercapita, inflation]  # Functions without ticker requirement
    file_paths = []
    for func in functions_to_run:
        df = func()  # Assuming each function returns a DataFrame
        filename = f"{func.__name__}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(filepath, index=False)
        file_paths.append(filepath)
        print(f"Data from {func.__name__} saved to {filepath}")
    return file_paths

def save_data_for_funcs_with_ticker():
    functions_to_run_with_ticker = [eps, cp, rsi, sma]
    combined_dfs = {func.__name__: pd.DataFrame() for func in functions_to_run_with_ticker}
    top_stocks_df = fetch_and_get_top_stocks()
    file_paths = []

    for func in functions_to_run_with_ticker:
        for index, row in top_stocks_df.iterrows():
            ticker = row['Ticker']
            df2 = func(ticker)
            df2['Ticker'] = ticker  # Ensure the ticker symbol is included in the DataFrame
            combined_dfs[func.__name__] = pd.concat([combined_dfs[func.__name__], df2], ignore_index=True)

        filename = f"{func.__name__}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        combined_dfs[func.__name__].to_csv(filepath, index=False)
        file_paths.append(filepath)
        print(f"Combined data for {func.__name__} saved to {filepath}")
    return file_paths

with DAG(
    dag_id='fetch_and_get_top_stocks',
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:
    
    fetch_and_save_ticker = PythonOperator(
        task_id='fetch_and_save_ticker', 
        python_callable=save_tickers, 
        op_kwargs={'ticker_df': fetch_and_get_top_stocks()},
    )

    # Task to fetch and save data for functions without ticker
    save_data_wo_ticker_task = PythonOperator(
        task_id='save_data_wo_ticker',
        python_callable=save_data_for_funcs_without_ticker,
    )

    # Task to fetch and save data for functions with ticker
    save_data_w_ticker_task = PythonOperator(
        task_id='save_data_w_ticker',
        python_callable=save_data_for_funcs_with_ticker,
    )

    # Dynamically creating LocalFilesystemToGCSOperator tasks for uploading
    for func_name in ['dty', 'realgdppercapita', 'inflation', 'eps', 'cp', 'rsi', 'sma']:
        upload_task = LocalFilesystemToGCSOperator(
            task_id=f'upload_{func_name}_to_gcs',
            gcp_conn_id='is3107project',
            bucket='is3107_grp11',
            src=f'{OUTPUT_DIR}/{func_name}.csv',
            dst=f'data/{func_name}.csv'
        )

        if func_name in ['dty', 'realgdppercapita', 'inflation']:
            save_data_wo_ticker_task >> upload_task
        else:
            save_data_w_ticker_task >> upload_task
    
    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id='local_to_gcs',
        gcp_conn_id = 'is3107project',
        bucket='is3107_grp11',
        src='dags/data/top_5_ticker.csv',
        dst='top_5_tickers'
    )
    
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        source_objects=['top_5_tickers'],
        destination_project_dataset_table='is3107-418111.proj_data.top_5_tickers',
        schema_fields=[
            {'name': 'Ticker', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )
    
fetch_and_save_ticker >> local_to_gcs >> gcs_to_bq
