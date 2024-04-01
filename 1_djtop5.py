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
    
    top_tickers = [ticker[0] for ticker in ticker_market_caps[:5]]  # Extract top 10 tickers
    
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
