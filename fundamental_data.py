import requests
import pandas as pd
import numpy as np
import io
from time import sleep
from datetime import date
from datetime import datetime
from datetime import datetime
import os


apikey1 = '6AHG15XLB86AMCZX'
apikey2 = '9ZLATZ1IC6GHUVAK'
apikey3 = '6I2Y9UULKVW6XGX5'
apikey4 = 'RYL3ULY9P5ZOJVUX'

def dty(key):
    url = f'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval=daily&maturity=10year&apikey={key}' #changed to daily from default
    r = requests.get(url)
    data = r.json()
    time_series_data = data["data"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    return df.head(1000)


def eps(key):
    url = f'https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey={key}'
    r = requests.get(url)
    data = r.json()
    time_series_data = data["quarterlyEarnings"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    return df

def realgdppercapita(key):
    url = f'https://www.alphavantage.co/query?function=REAL_GDP_PER_CAPITA&apikey={key}'
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
    url = f'https://www.alphavantage.co/query?function=INFLATION&apikey={key}'
    r = requests.get(url)
    data = r.json()
     time_series_data = data["data"]
    # Convert to DataFrame
    df = pd.DataFrame(time_series_data).T
    # Reset index and rename columns
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)
    return df
    