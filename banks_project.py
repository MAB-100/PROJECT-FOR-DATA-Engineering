import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
import logging

# Initialize logging
LOG_FILE = "code_log.txt"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

def log_progress(message):
    logging.info(message)
    print(message)

# Task 1: Initialize Logging
log_progress("Task 1: Initialized logging.")

# Task 2: Extract data from the given URL
def extract():
    log_progress("Task 2: Starting data extraction from Wikipedia.")
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    response = requests.get(url)

    # print("\n\n\n\n\n\n\n\n")
    # print(response.text)
    # print("\n\n\n\n\n\n\n\n")

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # print("\n\n\n\n\n\n\n\n")
    # print(soup)
    # print("\n\n\n\n\n\n\n\n")

    # Locate the table by inspecting the webpage structure
    table = soup.find('table', {'class': 'wikitable'})
    # print("\n\n\n\n\n\n\n\n")
    # print(str(table))
    # print("\n\n\n\n\n\n\n\n")

    df = pd.read_html(str(table))[0]
    # print("\n\n\n\n\n\n\n\n")
    # print(df.columns)
    # print("\n\n\n\n\n\n\n\n")
    df = df[['Bank name', 'Market cap (US$ billion)']]
    df.columns = ['Name', 'MC_USD_Billion']
    
    # Convert Market Cap to numeric, handling non-numeric values
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    df = df.dropna().sort_values(by='MC_USD_Billion', ascending=False).head(10)
    log_progress("Task 2: Data extraction completed successfully.")
    return df

# Execute data extraction
df = extract()

# Task 3: Transform data by adding currency columns
def transform(df):
    log_progress("Task 3: Starting data transformation.")
    exchange_rates = pd.read_csv("exchange_rate.csv")
    usd_to_gbp = exchange_rates.loc[exchange_rates['Currency'] == 'GBP', 'Rate'].values[0]
    usd_to_eur = exchange_rates.loc[exchange_rates['Currency'] == 'EUR', 'Rate'].values[0]
    usd_to_inr = exchange_rates.loc[exchange_rates['Currency'] == 'INR', 'Rate'].values[0]

    # Adding new columns for each currency
    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * usd_to_gbp).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * usd_to_eur).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * usd_to_inr).round(2)
    
    log_progress("Task 3: Data transformation completed successfully.")
    return df

# Execute data transformation
df_transformed = transform(df)

# Task 4: Load data to CSV
def load_to_csv(df, output_path):
    log_progress("Task 4: Saving data to CSV file.")
    df.to_csv(output_path, index=False)
    log_progress("Task 4: Data saved to CSV successfully.")

# Execute loading to CSV
load_to_csv(df_transformed, './Largest_banks_data.csv')

# Task 5: Load data to SQLite Database
def load_to_db(df, db_name, table_name):
    log_progress("Task 5: Loading data to SQLite database.")
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    log_progress("Task 5: Data loaded to database successfully.")

# Execute loading to database
load_to_db(df_transformed, 'Banks.db', 'Largest_banks')

# Task 6: Running queries on the database table
def run_queries(db_name, table_name):
    log_progress("Task 6: Running queries on the database.")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Example queries
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY MC_USD_Billion DESC LIMIT 5;")
    top_5_banks = cursor.fetchall()
    log_progress(f"Top 5 Banks by Market Cap: {top_5_banks}")
    
    conn.close()
    log_progress("Task 6: Queries executed successfully.")

# Execute running queries
run_queries('Banks.db', 'Largest_banks')

# Task 7: Verify that log entries have been completed
log_progress("Task 7: Verifying log entries.")
with open(LOG_FILE, 'r') as file:
    logs = file.read()
    log_progress(f"Log file contents:\n{logs}")
log_progress("Task 7: Verification completed.")
