import yfinance as yf
import pandas as pd
from datetime import date
import os

MAX_COMPANIES = 25
OUTPUT_DIR = '/tmp'
OUTPUT_FILE = 'top_25_volume.csv'
WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

def fetch_snp500_tickers():
    tables = pd.read_html(WIKI_URL)
    tickers = tables[0]['Symbol'].str.upper().head(MAX_COMPANIES) 
    top_tickers_data = [(date.today(), ticker) for ticker in [tickers]]
    top_tickers_df = pd.DataFrame(top_tickers_data, columns=['Date', 'Ticker'])
    return top_tickers_df

def save_report(report_df, directory=OUTPUT_DIR, filename=OUTPUT_FILE):
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    report_df.to_csv(filepath, index=False)
    print(f"Report saved to {filepath}")

def process_data():
    top_tickers_report = fetch_snp500_tickers()
    save_report(top_tickers_report)

if __name__ == "__main__":
    process_data()
