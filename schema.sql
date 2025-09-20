-- schema.sql
-- Raw TLC trip table (simplified to cover required fields)
CREATE DATABASE IF NOT EXISTS nyc_taxi;
USE nyc_taxi;

CREATE TABLE IF NOT EXISTS trips_raw (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    vendor_id VARCHAR(8),
    pickup_datetime DATETIME,
    dropoff_datetime DATETIME,
    passenger_count INT,
    trip_distance DOUBLE,
    rate_code INT,
    store_and_fwd_flag VARCHAR(4),
    PULocationID INT,
    DOLocationID INT,
    payment_type VARCHAR(16),
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    total_amount DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes used for ingestion & queries
CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON trips_raw (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_pulocation ON trips_raw (PULocationID);
CREATE INDEX IF NOT EXISTS idx_fare_amount ON trips_raw (fare_amount);

-- version table to track applied schema changes
CREATE TABLE IF NOT EXISTS schema_versions (
    version VARCHAR(64) PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);
