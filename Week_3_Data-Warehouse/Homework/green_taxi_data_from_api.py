import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    file_urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet"
    ]


    # Initialize an empty list to store DataFrames for each month
    dataframes = []
    
    #Define column data types
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }

    
    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

# Use a for loop to download, decompress, and read each file, then append the DataFrame to the list
    for file_url in file_urls:     
    
        # Read the CSV data into a DataFrame with specified data types and date parsing
            df = pd.read_parquet(file_url)
        # Append the DataFrame to the list
            dataframes.append(df)

# Concatenate the DataFrames in the list along the rows (axis=0)
    result_df = pd.concat(dataframes, axis=0, ignore_index=True)
    result_df['lpep_pickup_date'] = result_df['lpep_pickup_datetime'].dt.date

    return result_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
