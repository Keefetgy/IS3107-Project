import requests
import pandas as pd
import numpy as np
import io
from time import sleep
from datetime import date
from datetime import datetime
from datetime import datetime
import os

apikey1 = 'L0S0FYJGSKZLWC8R' #adjusted closing
apikey2 = '63TMPZD2NJJ51WFA'
apikey3 = 'YBATNF7ZMC9EJ246'

def acp(): #daily volume found inside here?
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={key}'
    r = requests.get(url)
    data = r.json()

def rsi():
    url = 'https://www.alphavantage.co/query?function=RSI&symbol={ticker}&interval=daily&time_period=10&series_type=open&apikey={key}' #changed to daily
    r = requests.get(url)
    data = r.json()

def sma():
    url = 'https://www.alphavantage.co/query?function=SMA&symbol=IBM&interval=daily&time_period=10&series_type=open&apikey={key}' #changed to daily
    r = requests.get(url)
    data = r.json()
