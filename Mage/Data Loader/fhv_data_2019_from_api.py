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
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz",
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz",
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-03.csv.gz"
    ]



    # Initialize an empty list to store DataFrames for each month
    dataframes = []

    taxi_dtypes = {
                    'Dispatching_base_num': str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'SR_Flag ': str
        }

          
    # native date parsing 
    
# Use a for loop to download, decompress, and read each file, then append the DataFrame to the list
    for file_url in file_urls:     
    
        # Read the CSV data into a DataFrame with specified data types and date parsing
        # Read the CSV data into a DataFrame with specified data types and date parsing
        df = pd.read_csv(file_url, sep=',', compression='gzip', dtype=taxi_dtypes)
        # Append the DataFrame to the list
        dataframes.append(df)

# Concatenate the DataFrames in the list along the rows (axis=0)
    result_df = pd.concat(dataframes, axis=0, ignore_index=True)
   
    return result_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
