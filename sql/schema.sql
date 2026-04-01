-- Star Schema for NYC Taxi Data (Gold Layer)

CREATE TABLE IF NOT EXISTS dim_payment (
    payment_type INTEGER,
    payment_id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_vendor (
    "VendorID" INTEGER,
    vendor_key INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_date (
    date DATE,
    date_id INTEGER PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    weekday INTEGER
);

CREATE TABLE IF NOT EXISTS dim_location (
    "PULocationID" INTEGER,
    "DOLocationID" INTEGER,
    location_id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS fact_trips (
    vendor_key INTEGER REFERENCES dim_vendor(vendor_key),
    payment_id INTEGER REFERENCES dim_payment(payment_id),
    date_id INTEGER REFERENCES dim_date(date_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    trip_duration_min DOUBLE PRECISION
);
