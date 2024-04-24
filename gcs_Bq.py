import pandas as pd
from datetime import date
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
#from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
import os
import logging
import requests
import csv
import requests
import yfinance as yf
import os 

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


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
    


# Define DAG
with DAG('is3107_gcs_to_bq', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    start_task = PythonOperator(
        task_id='start_task', 
        python_callable=save_tickers, 
        op_kwargs={'ticker_df': fetch_and_get_top_stocks()},
    )
    

    gcs_to_bq_cp= GCSToBigQueryOperator(
        task_id='gcs_to_bq_cp',
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        source_objects=['data/cp.csv'],
        destination_project_dataset_table='is3107-418111.proj_data.cp',
        schema_fields=[
        {'name': 'Ticker', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'Open', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'High', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'Low', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'Close', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'Volume', 'type': 'INTEGER', 'mode': 'REQUIRED'}
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )

    gcs_to_bq_rsi= GCSToBigQueryOperator(
        task_id='gcs_to_bq_rsi',
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        source_objects=['data/rsi.csv'],
        destination_project_dataset_table='is3107-418111.proj_data.rsi',
        schema_fields=[
        {'name': 'Ticker', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'RSI', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )

    gcs_to_bq_eps= GCSToBigQueryOperator(
        task_id='gcs_to_bq_eps',
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        source_objects=['data/eps.csv'],
        destination_project_dataset_table='is3107-418111.proj_data.eps',
        
        schema_fields=[
        {'name': 'Ticker', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'fiscalDateEnding', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'reportedDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'reportedEPS', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'estimatedEPS', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'surprise', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'surprisePercentage', 'type': 'STRING', 'mode': 'REQUIRED'}
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )

    gcs_to_bq_sma= GCSToBigQueryOperator(
        task_id='gcs_to_bq_sma',
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        source_objects=['data/sma.csv'],
        destination_project_dataset_table='is3107-418111.proj_data.sma',
        
        schema_fields=[
        {'name': 'Ticker', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'SMA', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )

    gcs_to_bq_dty= GCSToBigQueryOperator(
        task_id='gcs_to_bq_dty',
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        source_objects=['data/dty.csv'],
        destination_project_dataset_table='is3107-418111.proj_data.dty',
        
        schema_fields=[
        {'name': 'Date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'Value', 'type': 'STRING', 'mode': 'REQUIRED'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )

    gcs_to_bq_cpi= GCSToBigQueryOperator(
        task_id='gcs_to_bq_cpi',
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        source_objects=['data/cpi.csv'],
        destination_project_dataset_table='is3107-418111.proj_data.cpi',
        
        schema_fields=[
        {'name': 'Date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'Value', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )

    gcs_to_bq_rgpc= GCSToBigQueryOperator(
        task_id='gcs_to_bq_rgpc',
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        source_objects=['data/realgdppercapita.csv'],
        destination_project_dataset_table='is3107-418111.proj_data.rgpc',
        
        schema_fields=[
        {'name': 'Date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'Value', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        ],
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True
    )

    join_query = """
    CREATE OR REPLACE TABLE `is3107-418111.proj_data.joinedtable`
    AS 
    select a.ticker,a.date,a.close,a.volume,b.rsi,d.sma,e.value as TreasuryYield,f.value as Cpi ,c.surprise as EPS_Diff,g.value as RealGdpPerCapita from is3107-418111.proj_data.cp a 
    left join is3107-418111.proj_data.rsi b on a.ticker=b.ticker and a.date=b.date
    left join is3107-418111.proj_data.sma d on d.ticker=a.ticker and d.date = a.date
    left join is3107-418111.proj_data.dty e on e.date=a.date
    left join (SELECT 
        Date,
        Value,
        LEAD(Date) OVER (ORDER BY Date) AS next_date
    FROM 
        `is3107-418111.proj_data.cpi`) as f on a.date >= f.Date
    AND (a.Date < f.next_date OR f.next_date IS NULL)
    left join (SELECT Ticker, reportedDate,
            LEAD(reportedDate) OVER (PARTITION BY Ticker ORDER BY reportedDate) AS next_date,reportedeps,estimatedeps,surprise,surprisepercentage
        FROM is3107-418111.proj_data.eps ) as c  ON a.Ticker = c.Ticker
    AND a.date >= c.reportedDate
    AND (a.Date < c.next_date OR c.next_date IS NULL)
    left join (SELECT 
        Date,
        Value,
        LEAD(Date) OVER (ORDER BY Date) AS next_date
    FROM 
        `is3107-418111.proj_data.rgpc`

    ) g on a.date >= g.Date
    AND (a.Date < g.next_date OR g.next_date IS NULL)
    ORDER BY 
    a.date DESC ,a.ticker ASC;

    """

    join_table = BigQueryExecuteQueryOperator(
        task_id='create_joined_table',
        sql=join_query,
        gcp_conn_id = 'is3107project',
        use_legacy_sql=False,
        
    )
    export_to_gcs = BigQueryToGCSOperator(
        task_id='export_to_gcs',
        source_project_dataset_table='is3107-418111.proj_data.joinedtable',
        destination_cloud_storage_uris=['gs://is3107_grp11/data/joinedtable.csv'],
        export_format='CSV',
        
        gcp_conn_id='is3107project',  # Connection ID to Google Cloud Platform
    )
    







    # Define task dependencies
    start_task>>gcs_to_bq_cp>>gcs_to_bq_rsi>>gcs_to_bq_eps>>gcs_to_bq_sma>>gcs_to_bq_dty>>gcs_to_bq_rgpc>>gcs_to_bq_cpi>>join_table>>export_to_gcs
