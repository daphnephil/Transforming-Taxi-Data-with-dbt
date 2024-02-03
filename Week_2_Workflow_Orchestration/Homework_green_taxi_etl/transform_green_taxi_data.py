import re
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(name):
    """
    Convert Camel Case to Snake Case.
    """
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    # Transformer block
        # Remove rows where passenger count or trip distance is zero
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
        
        # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns from Camel Case to Snake Case
    # Determine the columns requiring renaming
    columns_before = data.columns
    columns_after = [camel_to_snake(col) for col in data.columns]

    columns_to_rename = [col_before for col_before, col_after in zip(columns_before, columns_after) if col_before != col_after]

# Count the number of columns to be renamed
    num_columns_to_rename = len(columns_to_rename)

    print(f"Number of columns to be renamed: {num_columns_to_rename}")
    print("Columns to be renamed:", columns_to_rename)

    data.columns = [camel_to_snake(col) for col in data.columns]
    
    #finding existing values of vendor_id in the dataset

    print("Existing values of VendorID:", data['vendor_id'].unique())
   
        
    return data


@test
def test_output(output, *args):
    """
    Template code for testing the output of the block.
    """
    # Assertions
    assert (output['vendor_id'].isin([1, 2])).all(), "Assertion Error: vendor_id is not one of the existing values."
    assert (output['passenger_count'] > 0).all(), "Assertion Error: passenger_count is not greater than 0."
    assert (output['trip_distance'] > 0).all(), "Assertion Error: trip_distance is not greater than 0."


