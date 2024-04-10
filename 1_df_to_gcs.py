import os
import pandas as pd
from datetime import date, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from google.cloud import storage  # Ensure you have the google-cloud-storage library installed
import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import yfinance as yf

dtykey = '6AHG15XLB86AMCZX'
epskey = '9ZLATZ1IC6GHUVAK'
gdpkey = '6I2Y9UULKVW6XGX5'
inflationkey = 'RYL3ULY9P5ZOJVUX'
cpkey = 'L0S0FYJGSKZLWC8R' 
rsikey = '63TMPZD2NJJ51WFA'
smakey = 'YBATNF7ZMC9EJ246'

# Constants
OUTPUT_DIR = '/tmp'
WIKI_URL = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
MAX_COMPANIES = 25

def fetch_DJ30_tickers():
    tables = pd.read_html(WIKI_URL)
    
    # The table containing the tickers is usually the first table
    tickers = tables[1]['Symbol'].str.upper()  # Check the index of the table
    
    top_tickers_data = [(date.today(), ticker) for ticker in tickers]
    top_tickers_df = pd.DataFrame(top_tickers_data, columns=['Date', 'Ticker'])
    return top_tickers_df

def fetch_and_get_top_stocks():
    WIKI_URL = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
    tables = pd.read_html(WIKI_URL)
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

def dty():
    url = f'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval=daily&maturity=10year&apikey={dtykey}' #changed to daily from default
    r = requests.get(url)
    data = r.json()
    time_series_data = data["data"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    return df.head(1000)


def eps(ticker):
    url = f'https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey={epskey}'
    r = requests.get(url)
    data = r.json()
    time_series_data = data["quarterlyEarnings"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    return df

def realgdppercapita():
    url = f'https://www.alphavantage.co/query?function=REAL_GDP_PER_CAPITA&apikey={gdpkey}'
    r = requests.get(url)
    data = r.json()
    time_series_data = data["data"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    return df
    

def inflation():
    url = f'https://www.alphavantage.co/query?function=INFLATION&apikey={inflationkey}'
    r = requests.get(url)
    data = r.json()
    time_series_data = data["data"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    return df

def cp(ticker):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={cpkey}'
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
    url = f'https://www.alphavantage.co/query?function=RSI&symbol={ticker}&interval=daily&time_period=10&series_type=open&apikey={rsikey}' #changed to daily
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
    url = f'https://www.alphavantage.co/query?function=SMA&symbol=IBM&interval=daily&time_period=10&series_type=open&apikey={smakey}' #changed to daily
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

def fetch_process_data():
    """
    Fetches and processes data from various functions defined in fundamental_data.py and technical_data.py.
    Saves each dataframe to the OUTPUT_DIR with a unique filename.
    """
    functions_to_run_with_ticker = [eps, cp, rsi, sma]
    functions_to_run = [dty, realgdppercapita, inflation]  # Example functions
    for func in functions_to_run:
        df = func()  # Assuming each function returns a DataFrame
        filename = f"{func.__name__}_{date.today()}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        df.to_csv(filepath, index=False)
        print(f"Data from {func.__name__} saved to {filepath}")
        # Upload to GCS
        upload_to_gcs(bucket_name='is3107_grp11', source_file_name=filepath, destination_blob_name=f"data/{filename}")

    for func in functions_to_run_with_ticker:
        df = fetch_and_get_top_stocks()
        for index, row in df.iterrows():
            ticker = row['Ticker']
            df2 = func(ticker)
            filename = f"{ticker}_{func.__name__}_{date.today()}.csv"
            filepath = os.path.join(OUTPUT_DIR, filename)
            df2.to_csv(filepath, index=False)
            print(f"Data from {ticker}_{func.__name__} saved to {filepath}")
            # Upload to GCS
            upload_to_gcs(bucket_name='is3107_grp11', source_file_name=filepath, destination_blob_name=f"data/{filename}")

# Function to upload files to GCS
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    # Initialize the GCS hook
    gcs_hook = GCSHook(gcp_conn_id='is3107project')
    
    # Use the hook to upload file to GCS
    gcs_hook.upload(bucket_name, destination_blob_name, source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# Airflow DAG
with DAG(
    dag_id="fetch_and_process_data",
    start_date=datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_fetch_process_data = PythonOperator(
        task_id='fetch_process_data',
        python_callable=fetch_process_data
    )
