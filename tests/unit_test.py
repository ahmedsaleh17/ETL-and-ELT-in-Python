import pandas as pd
from etl_utils import  extract_from_csv, extract_from_parquet, extract_from_json, transform_df_json_based

def test_extract_csv():
    """testing extraction csv file"""

    scores_data  = extract_from_csv("data-sources/scores.csv")
    
    assert isinstance(scores_data, pd.DataFrame)


def test_extract_parquet():
    """testing extraction parquet file"""

    scores_data  = extract_from_parquet("data-sources/scores.parquet")
    
    assert isinstance(scores_data, pd.DataFrame)


def test_extract_json():
    """testing extraction json file"""

    scores_data  = extract_from_json("data-sources/testing_scores.json")
    
    assert isinstance(scores_data, pd.DataFrame)



def test_transform_df_json_based():
    """Test the output of this function"""
    df_json = extract_from_json('data-sources/testing_scores.json')
    transformed_df = transform_df_json_based(df_json)

    # check datatype of the output 
    assert isinstance(transformed_df, pd.DataFrame)
    # check the number of columns 
    assert len(transformed_df.columns == 6)
