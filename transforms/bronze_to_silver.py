    # transforms/bronze_to_silver.py

import os
from pathlib import Path
import json
from loguru import logger

from pipeline_config import (
    BRONZE_DIR,
    SILVER_DIR,
    LOG_LEVEL,
    BRONZE_SILVER_CHECKPOINT,
)

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
        

def discover_new_bronze_files(bronze_dir: Path, processed_files: set) -> list[Path]:
    """
    Scans the Hive tree for all Parquet files and filters out 
    any relative paths we have already processed.
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
    


def read_latest_bronze_batches(bronze_dir: Path):
    """
    read the raw Parquet files in hive structure 
    Goal for later: Load the raw data into memory so we can see what we're working with.
    """
    logger.info(f"Scanning Bronze directory: {bronze_dir}")
    # TODO: Implement file system scanning
    pass


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
    files_to_process = discover_new_bronze_files(Path(BRONZE_DIR), processed_history)

    # --- Terminal Checkpoint ---
    if files_to_process:
        logger.info("Sample of files found for processing:")
        for file in files_to_process[:3]:  # Print up to the first 3 files
            print(f"Found: {file.name} in directory: {file.parent}")
    else:
        logger.warning("No new files found to process. Exiting early.")
        return

    # Rest of the pipeline stubs remain paused for now...
    logger.success("Step 1 Checkpoint Successful!")






    
    logger.success("Transformation pipeline completed successfully!")

if __name__ == "__main__":
    main()