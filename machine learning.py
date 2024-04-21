import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import pandas as pd
import os
from sklearn.pipeline import Pipeline
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
from datetime import date
from datetime import datetime
import yfinance as yf
import os
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import requests

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
    

def is_unwanted(val):
    if pd.isnull(val):
        return False
    if isinstance(val, str) and ('.' in val):
        return False
    return True

def process_data(): 
    df = pd.read_csv(os.path.join(OUTPUT_DIR, 'joinedtable.csv'))
    df['next_day_close'] = df.groupby('ticker')['close'].shift(1)
    df['price_increase'] = (df['next_day_close'] - df['close'] > 0).astype(int)
    df.replace('.', pd.NA, inplace= True)
    df.dropna(inplace=True)
    df['TreasuryYield'] = df['TreasuryYield'].astype(float)
    df['date'] = pd.to_datetime(df['date'])
    df.to_csv(os.path.join(OUTPUT_DIR, 'cleaned_joinedtable.csv'),index=False)

def run_ml():
    df = pd.read_csv(os.path.join(OUTPUT_DIR, 'cleaned_joinedtable.csv'))
    latest_date = df['date'].max()
    test_data = df[df['date'] == latest_date]
    training_data = df[df['date'] != latest_date]
    X = training_data.drop(columns=[training_data.columns[0], training_data.columns[1],training_data.columns[10],training_data.columns[11]])
    y = training_data.iloc[:, 11]  
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    test_X = test_data.drop(columns=[test_data.columns[0], test_data.columns[1],test_data.columns[10],test_data.columns[11]])
    test_y = test_data.iloc[:, 11] 
    params = {
        'learning_rate': 0.05,
        'max_depth': 15,
        'n_estimators': 300}
    classifier = Pipeline([
        ('scaler', StandardScaler()),
        ('classifier', GradientBoostingClassifier(**params, random_state=42))])
    classifier.fit(X, y)
    predictions = classifier.predict(test_X)
    test_data['Predictions'] = predictions
    test_data.drop(columns=['price_increase'], inplace=True)
    test_data.to_csv(os.path.join(OUTPUT_DIR, 'finaltable.csv'),index=False)

with DAG('is3107_machinelearning_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    start_task = PythonOperator(
        task_id='start_task', 
        python_callable=save_tickers, 
        op_kwargs={'ticker_df': fetch_and_get_top_stocks()},
    )

    gcs_to_local = GoogleCloudStorageDownloadOperator(
        task_id = "gcs_to_local",
        gcp_conn_id='is3107project',
        bucket='is3107_grp11',
        object_name='data/joinedtable.csv',  # Corrected the path here
        filename=os.path.join(OUTPUT_DIR, 'joinedtable.csv')  # Correctly formatted filename
    )

    task_process_data = PythonOperator(
         task_id = 'process_data',
         python_callable=process_data,
         provide_context=True
    )

    task_run_ml = PythonOperator(
         task_id = 'run_ml',
         python_callable=run_ml,
         provide_context=True
    )

    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id='local_to_gcs',
        gcp_conn_id = 'is3107project',
        bucket='is3107_grp11',
        src='dags/data/finaltable.csv',
        dst='data/finaltable.csv'
    )

start_task >> gcs_to_local >> task_process_data >> task_run_ml >> local_to_gcs

