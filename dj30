import pandas as pd
from datetime import date
import yfinance as yf

WIKI_URL = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'

def fetch_DJ30_tickers():
    tables = pd.read_html(WIKI_URL)
    
    # The table containing the tickers is usually the first table
    tickers = tables[1]['Symbol'].str.upper()  # Check the index of the table
    
    top_tickers_data = [(date.today(), ticker) for ticker in tickers]
    top_tickers_df = pd.DataFrame(top_tickers_data, columns=['Date', 'Ticker'])
    return top_tickers_df
