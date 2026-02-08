import logging
import time

from etl_utils import (
    extract_from_csv,
    extract_from_parquet,
    extract_from_json,
    transform_df_json_based,
    transform,
    load_to_csv,
    load_to_parquet,
)

# Logging data pipeline performance
logging.basicConfig(
    filename="pipeline.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
)


if __name__ == "__main__":

    data_path1 = "data-sources/scores.csv"
    data_path2 = "data-sources/scores.parquet"
    data_path3 = "data-sources/testing_scores.json"

    # very simple automation :)

    # run pipeline
    try:
        dest_file = f"data-sources/scores_cleaned.csv"

        # extracted_data = extract_from_csv(file_name)

        # first pipeline

        # reading data from parquet, transform this data and then load to csv
        extracted_data = extract_from_parquet(data_path2)
        clean_data = transform(extracted_data)
        load_to_csv(clean_data, dest_file)

        # add logs
        logging.info(
            f"First Data Pipeline Running successfully and data are loaded into {dest_file}."
        )

        logging.debug(
            f"Shape of the DataFrame before Transformation: {extracted_data.shape}"
        )

        logging.debug(
            f"Shape of the DataFrame after Transformation: {clean_data.shape}"
        )

        # increase version

        time.sleep(10)

    except Exception as e:
        logging.error(f"{e} arose in execution in first pipeliney")

    # run the second pipeline that ingest json data, then transform it and load it as csv file
    try:
        # Extract
        df_json = extract_from_json(data_path3)
        # Transform
        df_clean = transform_df_json_based(df_json)
        # Load
        load_to_csv(df_clean, "data-sources/testing_scores.csv")
        logging.info(
            "Second Data Pipeline Running successfully and testing scores json data are loaded into testing_scores.csv"
        )

        logging.debug(f"Shape of the DataFrame before Transformation: {df_json.shape}")
        logging.debug(f"Shape of the DataFrame after Transformation: {df_clean.shape}")
        logging.debug(
            f"""Checking the numbers of null values after runnign pipeline:
                      \t \t  Total Null values in testing_scores data is: {df_clean.isna().sum().sum()}"""
        )

    except Exception as e:
        logging.error(f"{e} arose in execution in the second pipeline")
