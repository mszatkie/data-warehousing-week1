-- Drop tables if they exist
DROP TABLE IF EXISTS central_park_weather;
DROP TABLE IF EXISTS fhv_bases;
DROP TABLE IF EXISTS yellow_tripdata;
DROP TABLE IF EXISTS green_tripdata;
DROP TABLE IF EXISTS fhv_tripdata;
DROP TABLE IF EXISTS fhvhv_tripdata;

-- Create table for central_park_weather
CREATE TABLE IF NOT EXISTS central_park_weather (
    date DATE,
    temperature FLOAT,
    precipitation FLOAT,
    snowfall FLOAT
);

-- Create table for fhv_bases
CREATE TABLE IF NOT EXISTS fhv_bases (
    base_number TEXT,
    base_name TEXT,
    base_type TEXT
);

-- Create table for yellow_tripdata
CREATE TABLE IF NOT EXISTS yellow_tripdata (
    vendor_id TEXT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    rate_code_id INTEGER,
    store_and_fwd_flag TEXT,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT
);

-- Create table for green_tripdata
CREATE TABLE IF NOT EXISTS green_tripdata (
    vendor_id TEXT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    rate_code_id INTEGER,
    store_and_fwd_flag TEXT,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT
);

-- Create table for fhv_tripdata
CREATE TABLE IF NOT EXISTS fhv_tripdata (
    dispatching_base_number TEXT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    SR_Flag INTEGER
);

-- Create table for fhvhv_tripdata
CREATE TABLE IF NOT EXISTS fhvhv_tripdata (
    hvfhs_license_num TEXT,
    dispatching_base_num TEXT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    SR_Flag INTEGER
);
