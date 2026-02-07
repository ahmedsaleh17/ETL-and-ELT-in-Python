"""
Docstring for etl-utils
build all function of Extraction, Transformation and Loading data 

"""
import pandas as pd 


def extract_from_csv(file_path):
    """
    Docstring for extract_from_csv
    
    :param file_path: the path of the csv file you want to ingest
    :return : A Pandas DataFrame 
    """
    return pd.read_csv(file_path)


def extract_from_parquet(file_path):
    """
    Docstring for extract_from_parquet
    
    :param file_path: the path of Parquet file you want to ingest 
    :return: A Pandas DataFrame 
    """
    return pd.read_parquet(file_path, engine= 'fastparquet')


def transform(data_frame):
    """
    This function take the raw input data frame 
    converting all percentage columns to float data type.

    :param data_frame: the raw input data frame 
    :return : a transformed data frame
    """
    columns_to_edit = ['Percent Black', 'Percent Hispanic', 'Percent Asian', 'Percent Tested']
    for col in columns_to_edit:
        data_frame[col] = data_frame[col].str.strip("%").astype('float')
        data_frame = data_frame.fillna(value = 0)
    return data_frame[['School Name', 'Student Enrollment', *columns_to_edit ]]

def load_to_csv(transformed_df, destination_file):
    """
    Docstring for load_to_csv
    
    :param transformed_df: The transformed Data frame wanted to loaded.
    :param destination_file: The path of csv file you want to load data into.
    """
    transformed_df.to_csv(destination_file)


def load_to_parquet(transformed_df, destination_file):
    """
    Docstring for load_to_parquet
    
    :param transformed_df: The transformed Data frame wanted to loaded.
    :param destination_file: The path of parquet file you want to load data into.
    """
    transformed_df.to_parquet(destination_file)
