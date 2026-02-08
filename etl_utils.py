"""
Docstring for etl-utils
build all function of Extraction, Transformation and Loading data

"""

import pandas as pd
import json


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
    return pd.read_parquet(file_path, engine="fastparquet")


def extract_from_json(file_path):
    """
    Docstring for extract_from_json

    :param file_path: the path of the JSON file you want to ingest
    :return: A Pandas DataFrame
    """
    # Load the data and orient by index (rows)
    df = pd.read_json(file_path, orient="index")
    return df


def transform_df_json_based(dataframe):
    """
    Docstring for transform_df_json_based

    Flattens nested 'scores' column into individual columns and restructures the dataframe.

    :param dataframe: the raw input dataframe with nested 'scores' column
    :return: A transformed dataframe with flattened scores and reset index
    """
    # Flatten the nested 'scores' column into individual columns
    scores_df = dataframe["scores"].apply(pd.Series)
    # drop scores column from the dataframe
    dataframe.drop(columns="scores", inplace=True)

    # now concat the dataframe with scores_df
    df = pd.concat([dataframe, scores_df], axis=1)
    # rename the index
    df.index.name = "scores_id"
    # reset the index
    df.reset_index(inplace=True)

    # replace nulls by the average values
    cols = ["math", "reading", "writing"]
    df[cols] = df[cols].fillna(df[cols].mean().round())

    return df


def transform(data_frame):
    """
    This function take the raw input data frame
    converting all percentage columns to float data type.

    :param data_frame: the raw input data frame
    :return : a transformed data frame
    """
    columns_to_edit = [
        "Percent Black",
        "Percent Hispanic",
        "Percent Asian",
        "Percent Tested",
    ]
    for col in columns_to_edit:
        data_frame[col] = data_frame[col].str.strip("%").astype("float")
        data_frame = data_frame.fillna(value=0)
    return data_frame[["School Name", "Student Enrollment", *columns_to_edit]]


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


if __name__ == "__main__":
    with open("data-sources/all_data.json") as f:
        json_data = json.load(f)

    with open("data-sources/testing_scores.json", "w") as json_file:
        json.dump(json_data, json_file, indent=4)
