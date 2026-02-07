import logging
import time

from etl_utils import (
    extract_from_csv,
    extract_from_parquet,
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

    version = 1
    file_name = "data-sources/scores.csv"
    file_name = "data-sources/scores.parquet"

    # very simple automation :)
    
    # run pipeline every 20 seconds
    while True:

        if version == 6:
            break

        # run pipeline
        try:
            dest_file = f"data-sources/scores_cleaned_v{version}.csv"
            
            # extracted_data = extract_from_csv(file_name)

            # reading data from parquet, transform this data and then load to csv 
            extracted_data = extract_from_parquet(file_name)
            clean_data = transform(extracted_data)
            load_to_csv(clean_data, dest_file)

            # add logs
            logging.info(
                f"Data Pipeline Running successfully and data are loaded into {dest_file}."
            )

            # increase version
            version += 1
            time.sleep(20)

        except Exception as e:
            logging.error(f"{e} arose in execution")
            
            break