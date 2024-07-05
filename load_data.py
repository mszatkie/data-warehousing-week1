import duckdb
import os
import requests

# Connect to DuckDB database (main.db)
con = duckdb.connect('main.db')

# Define the base URL for the S3 bucket
base_url = 'https://s3.us-east-1.amazonaws.com/cmu-95797-23m2/'

# List of identified CSV files to download
csv_files = [
    'data/central_park_weather.csv',
    'data/fhv_bases.csv'
]

# Create a directory to store the downloaded data
data_directory = './data/'
os.makedirs(data_directory, exist_ok=True)

# Function to download and save the CSV files
def download_file(url, local_filename):
    """
    Downloads a file from a specified URL and saves it locally.

    Args:
    url (str): The URL of the file to download
    local_filename (str): The path where the file will be saved

    Returns:
    None
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

# Download each CSV file
for csv_file in csv_files:
    file_url = base_url + csv_file
    local_filename = os.path.join(data_directory, os.path.basename(csv_file))
    download_file(file_url, local_filename)

# Load data from CSV files into DuckDB
for csv_file in csv_files:
    local_filename = os.path.join(data_directory, os.path.basename(csv_file))
    table_name = os.path.basename(csv_file).split('.')[0]
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{local_filename}')")

# Verify the row counts for each table in the DuckDB database
for table_name in con.execute("SHOW TABLES").fetchall():
    result = con.execute(f"SELECT COUNT(*) FROM {table_name[0]}").fetchall()
    print(f"Table: {table_name[0]}, Row count: {result[0][0]}")
