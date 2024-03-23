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

def dty():
    url = 'https://www.alphavantage.co/query?function=TREASURY_YIELD&interval=daily&maturity=10year&apikey={key}' #changed to daily from default
    r = requests.get(url)
    data = r.json()


def eps():
    url = 'https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey={key}'
    r = requests.get(url)
    data = r.json()

def gdp():
    url = 'https://www.alphavantage.co/query?function=REAL_GDP&interval=quarterly&apikey={key}'
    r = requests.get(url)
    data = r.json()

def inflation():
    url = 'https://www.alphavantage.co/query?function=INFLATION&apikey={key}'
    r = requests.get(url)
    data = r.json()