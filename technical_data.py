import requests
import pandas as pd
import numpy as np
import io
from time import sleep
from datetime import date
from datetime import datetime
from datetime import datetime
import os

apikey1 = 'L0S0FYJGSKZLWC8R' 
apikey2 = '63TMPZD2NJJ51WFA'
apikey3 = 'YBATNF7ZMC9EJ246'

def cp(ticker,key):
    #url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={}'
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&outputsize=full&apikey=demo' # for demo data
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

def rsi(ticker,key ):
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

def sma(ticker,key):
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

