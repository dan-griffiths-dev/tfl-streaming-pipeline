    # transforms/bronze_to_silver.py

import os
from pathlib import Path
import json
import sys


from loguru import logger

from pyspark.sql import SparkSession

from pipeline_config import (
    BRONZE_DIR,
    SILVER_DIR,
    LOG_LEVEL,
    BRONZE_SILVER_CHECKPOINT,
)

# Force PySpark to use active uv Python environment binary
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Hardcode the macOS Homebrew Java 11 path directly into the environment
# This ensures Spark's JVM finds the correct Java 11 runtime on launch
mac_java_11_path = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
if Path(mac_java_11_path).exists():
    os.environ["JAVA_HOME"] = mac_java_11_path

# eventually use DuckDB or Pandas for this later, but let's keep imports clean for now.


# build checkpoint layer
# scan the directory, 
# look at the checkpoint, 
# identify new files, 
# print list of new filepaths to the terminal.


def load_processed_files() -> set:
    """
    Loads the set of relative file paths that have already been cleaned.
    Returns an empty set if it's the first run.
    """
    # fail path: first run or file doesn't exist
    checkpoint_path = Path(BRONZE_SILVER_CHECKPOINT)
    if not checkpoint_path.exists():
        logger.info(f"Checkpoint file not found ({checkpoint_path}). Starting fresh first-run.")
        return set()
    
    try:
        # open and read checkpoint file into a python object with json.loads()
        with open(checkpoint_path, "r") as f:
            processed_list = json.load(f)
            logger.info(f"Loaded {len(processed_list)} processed files from checkpoint.")
            return set(processed_list)

    except Exception as e:
        logger.error(f"Failed to read checkpoint file, defaulting to empty. Error: {e}")
        return set()
        

def find_new_bronze_files(bronze_dir: Path, processed_files: set) -> list[Path]:
    """
    Scans the Hive tree for all Parquet files and filters out 
    any relative paths already processed.
    """

    logger.info(f"Scanning for new data in: {bronze_dir}")
    
    # get all parquet files inside the nested Hive folders
    all_parquet_files = list(Path(bronze_dir).rglob("*.parquet"))
    logger.info(f"Found total of {len(all_parquet_files)} Parquet files on disk.")

    new_files = []
    for file_path in all_parquet_files:
        # shorten absolute path to a relative path from the bronze root
        # 'year=2026/month=06/day=18/batch_1.parquet'
        relative_path = str(file_path.relative_to(bronze_dir))
        
        if relative_path not in processed_files:
            new_files.append(file_path)

    logger.success(f"Discovery complete. Identified {len(new_files)} new files to process.")
    return new_files
    

def read_latest_bronze_batches(file_paths):
    """
    read the raw Parquet files in hive structure 
    Goal for later: Load the raw data into memory to see output
    """
    logger.info(f"Scanning Bronze directory")
    

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("tfl-bus-data-lake-bronze-to-silver")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")


    logger.info("Starting Bronze -> Silver transformation with PySpark")
    # Note, file paths must be str, ensure not posix object
    file_paths = [
        str(path) for path in file_paths
    ]

    df_bronze = spark.read.parquet(*file_paths)

    return df_bronze






def flatten_and_cast_schema():
    """
    Step 2: Break open nested JSON structures and fix data types.
    Goal for later: Convert string timestamps to actual Datetime objects, unpack nested arrays.
    """
    # TODO: Implement schema transformation
    pass


def deduplicate_records():
    """
    Step 3: Remove duplicate streaming records.
    Goal for later: Ensure each unique vehicle arrival is only counted once based on an ID or timestamp.
    """
    # TODO: Implement deduplication logic
    pass


def write_to_silver_lake(silver_dir: Path):
    """
    Step 4: Save the pristine data back down to disk.
    Goal for later: Write out clean Hive-partitioned Parquet files into data/silver/.
    """
    # TODO: Implement silver write
    pass


def main():
    """
    The orchestrator that glues the steps together.
    """
    logger.info("Starting Bronze to Silver transformation pipeline...")
    
    # Load the checkpoint history
    processed_history = load_processed_files()

    # Find files that haven't been touched yet
    files_to_process = find_new_bronze_files(Path(BRONZE_DIR), processed_history)
    
    # Load into Spark
    df_bronze = read_latest_bronze_batches(files_to_process)
    
    # --- Terminal Checkpoint ---
    logger.info("Printing raw Bronze schema from Spark:")
    df_bronze.printSchema()
    
    logger.info("peekraw rows:")
    df_bronze.show(5, truncate=False)
    
    logger.success("Step 2 Checkpoint Successful! Schema loaded into memory.")


    # logger.success("Transformation pipeline completed successfully!")

if __name__ == "__main__":
    main()