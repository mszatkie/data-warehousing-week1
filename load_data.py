import duckdb
import os
import requests

# Connect to DuckDB database (main.db)
con = duckdb.connect('main.db')

# Define the base URL for the S3 bucket
base_url = 'https://s3.us-east-1.amazonaws.com/cmu-95797-23m2/'

# List of identified CSV and Parquet files to download
files = [
    'data/central_park_weather.csv',
    'data/fhv_bases.csv',
    'data/taxi/yellow_tripdata.parquet',
    'data/taxi/green_tripdata.parquet',
    'data/taxi/fhv_tripdata.parquet',
    'data/taxi/fhvhv_tripdata.parquet',
    'data/citibike-tripdata.csv.gz'
]

# Create a directory to store the downloaded data
data_directory = './data/'
os.makedirs(data_directory, exist_ok=True)

# Function to download and save the files
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

# Download each file
for file in files:
    file_url = base_url + file
    local_filename = os.path.join(data_directory, os.path.basename(file))
    download_file(file_url, local_filename)

# Load data from files into DuckDB
for file in files:
    local_filename = os.path.join(data_directory, os.path.basename(file))
    table_name = os.path.basename(file).split('.')[0].replace('-', '_')
    # Drop the table if it already exists
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    if file.endswith('.csv') or file.endswith('.csv.gz'):
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{local_filename}')")
    elif file.endswith('.parquet'):
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{local_filename}')")

# Verify the row counts for each table in the DuckDB database
for table_name in con.execute("SHOW TABLES").fetchall():
    result = con.execute(f"SELECT COUNT(*) FROM {table_name[0]}").fetchall()
    print(f"Table: {table_name[0]}, Row count: {result[0][0]}")
