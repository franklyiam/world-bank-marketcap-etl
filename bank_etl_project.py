# Code for ETL operations on World Banks data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# -----------------------Initialization-----------------------------#

# Initialize all the known entities
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs_extraction = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
load_csv_path = './Largest_banks_data.csv'
exchange_csv_path = './exchange_rate.csv'
log_file = './code_log.txt'

# -------------------------Log Function ---------------------------- #
''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
##record a message, along with its timestamp, in the log_file
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ':' + message + '\n') 

# -----------------------Extraction-----------------------------#   
''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
def extract(url, table_attribs_extraction):
    # Log the start of the Extraction process of the ETL pipeline 
    log_progress("Logging...... Extraction Process Started")
    # Loading the webpage for Webscraping
    html_page = requests.get(url).text
    #Parse the text into an HTML object
    data = BeautifulSoup(html_page, 'html.parser')
    #Create an empty pandas DataFrame named df with columns as the table_attribs
    df = pd.DataFrame(columns=table_attribs_extraction)
    #Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 0 table using the 'tr' attribute.
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    '''Check the contents of each row, having attribute ‘td’, for the following conditions.
        a. The row should not be empty.
        b. The first column should contain a hyperlink.
        c. Remove newline characters ("\n") from the data in the second column'''
    for row in rows:
        col = row.find_all('td')
        if len(col) !=0:
            dirty_MC_value = col[2].contents[0]
            cleaned_MC_value = dirty_MC_value.replace("\n", "").strip()
            data_dict = {"Name": col[1].contents[2].contents[0], "MC_USD_Billion": float(cleaned_MC_value)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    # Log the end of the Extraction process of the ETL pipeline 
    log_progress("Logging...... Extraction Process Complete")
    return df

# -----------------------Transformation-----------------------------#
''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
def transform(df, exchange_csv_path):
    # Log the start of the Transformation process of the ETL pipeline 
    log_progress("Logging...... Transformation Process Started")
    #read the exchange_rate.csv file
    exchange_df = pd.read_csv(exchange_csv_path)
    #convert the exchange_rate dataframe into a dictionary
    exchange_dict = exchange_df.set_index('Currency').to_dict()['Rate'] 
    #scale each MC value to it's corresponding exchange rate from the exchange_dict & round off to two decimal places
    # 1 : adding the MC_GBP_Billion column (Great British Pound)
    df['MC_GBP_Billion'] = [np.round(x*exchange_dict['GBP'],2) for x in df['MC_USD_Billion']]
    # 2 : adding the MC_EUR_Billion column (Euros)
    df['MC_EUR_Billion'] = [np.round(x*exchange_dict['EUR'],2) for x in df['MC_USD_Billion']]
    # 3 : adding the MC_INR_Billion column (Indian Rupee)
    df['MC_INR_Billion'] = [np.round(x*exchange_dict['INR'],2) for x in df['MC_USD_Billion']]
    # Log the end of the Transformation process of the ETL pipeline 
    log_progress("Logging...... Transformation Process Completed")
    return df

# -----------------------Load-----------------------------#
''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
def load_to_csv(df, load_csv_path):
    df.to_csv(load_csv_path)
    # Log the load to CSV process of the ETL pipeline 
    log_progress("Logging...... Load to CSV Process Completed")

''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    # Log the load to SQL Database process of the ETL pipeline 
    log_progress("Logging...... Load to SQL Database Process Completed")
    
''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
# -----------------------Query the database-----------------------------#
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    print("\n")
    log_progress("Logging...... Query Execution Complete")

# -----------------------Running the ETL Pipeline-----------------------------#
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs_extraction)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, exchange_csv_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, load_csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

# 1 SQL Query: Print entire table
query_statement = f"SELECT * from {table_name} "
run_query(query_statement, sql_connection)

# 2 SQL Query: Print average market capitalization of all banks
query_statement = f"SELECT AVG(MC_GBP_Billion) from {table_name} "
run_query(query_statement, sql_connection)

# 3 SQL Query: Print names of top 5 banks
query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()