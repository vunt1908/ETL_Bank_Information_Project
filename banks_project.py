import numpy as np
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributes_extract = ['Name', 'MC_USD_Billion']
table_atributes_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'
csv_path = 'exchange_rate.csv'
output_path = 'Largest_banks_data.csv'

#Logging
def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Year-Month-Day Hour:Minute:Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + " : " + message + '\n')

log_progress("Preliminaries complete. Initiating ETL process")

# Extract data
def extract(url, table_attributes_extract):
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')

    df = pd.DataFrame(columns=table_attributes_extract)
    table = soup.find_all('table')
    rows = table[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                'Name' : col[1].text.strip(),
                'MC_USD_Billion' : float(col[2].contents[0].replace('\n', '').replace(',', ''))
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

extracted_data = extract(url, table_attributes_extract)
print(extracted_data)
log_progress("Data extraction complete. Initiating Transformation process")

# Transform data
def transform(df, csv_path):
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]

    return df

transformed_data = transform(extracted_data, csv_path)
print(transformed_data)
log_progress("Data transformation complete. Initiating Loading process")

# Load to CSV
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

load_to_csv(transformed_data, output_path)
log_progress("Data saved to CSV file")

# Load to DB
def load_to_db(df, connection, table_name):
    df.to_sql(table_name, connection, if_exists='replace', index=False)

connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(transformed_data, connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

#Run the query
def run_queries(query_statement, connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, connection)
    print(query_output)

query_statement1 = f"SELECT * FROM {table_name}"
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query_statement3 = f"SELECT Name from {table_name} LIMIT 5"

run_queries(query_statement1, connection)
run_queries(query_statement2, connection)
run_queries(query_statement3, connection)

log_progress("Process Complete")

connection.close()
log_progress("Server Connection closed")