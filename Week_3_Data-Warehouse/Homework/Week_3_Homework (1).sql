-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-380621.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://meme2_mage_zoomcamp/green_taxi_2022.parquet']
);

-- Check green trip data
SELECT * FROM dtc-de-course-380621.ny_taxi.external_green_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-course-380621.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM dtc-de-course-380621.ny_taxi.external_green_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-course-380621.ny_taxi.green_tripdata_partitoned
PARTITION BY
  lpep_pickup_date AS
SELECT * FROM dtc-de-course-380621.ny_taxi.external_green_tripdata;

-- count the distinct number of PULocationIDs from external table
SELECT COUNT(DISTINCT pulocation_id) as distinct_pu_location_ids
FROM dtc-de-course-380621.ny_taxi.external_green_tripdata;

-- count the distinct number of PULocationIDs from the non-partitioned table
SELECT COUNT(DISTINCT pulocation_id) as distinct_pu_location_ids
FROM dtc-de-course-380621.ny_taxi.green_tripdata_non_partitoned ;
