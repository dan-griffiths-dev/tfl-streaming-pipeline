    # transforms/bronze_to_silver.py

import os
from pathlib import Path
import json
import sys


from loguru import logger

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window



from pipeline_config import (
    BRONZE_DIR,
    SILVER_DIR,
    LOG_LEVEL,
    BRONZE_SILVER_CHECKPOINT,
)


# ==========================================
# 1. ENVIRONMENT CONFIGURATION (Java 11 Fix)
# ==========================================
# Force PySpark to use active uv Python environment binary
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Hardcode the macOS Homebrew Java 11 path directly into the environment
# This ensures Spark's JVM finds the correct Java 11 runtime on launch
mac_java_11_path = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
if Path(mac_java_11_path).exists():
    os.environ["JAVA_HOME"] = mac_java_11_path

# ==========================================
# 2. DATA PIPELINE LOGIC 
# ==========================================

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
    # Note, change file paths to be str, ensure not posix object
    file_paths = [
        str(path) for path in file_paths
    ]

    df_bronze = spark.read.parquet(*file_paths)

    return df_bronze


def transform_bronze_to_silver(df_bronze):
    logger.info("Executing Silver layer transformation pipeline...")

    # Step 1: Type casting
    df_typed = df_bronze.withColumn(
        "api_timestamp", F.to_timestamp("api_timestamp")
    ).withColumn(
        "expected_arrival", F.to_timestamp("expected_arrival")
    ).withColumn(
        "time_to_station", F.col("time_to_station").cast("integer")
    )

    # Step 2: DEDUPLICATION FIX
    # We drop updates for the SAME bus at the SAME stop.
    # This keeps the earliest prediction for that station and eliminates trailing time-slippage noise.
    df_deduped = df_typed.dropDuplicates(["vehicle_id", "naptan_id"])

    # Step 3: Individual Vehicle Delay Window (Tracking baseline slippage across its life)
    trip_window = Window.partitionBy("vehicle_id", "trip_id", "line_id").orderBy("api_timestamp")
    
    df_with_delays = df_deduped.withColumn(
        "baseline_expected_arrival", F.first("expected_arrival").over(trip_window)
    ).withColumn(
        "delay_minutes",
        (F.col("expected_arrival").cast("long") - F.col("baseline_expected_arrival").cast("long")) / 60
    )

    # Step 4: Headway Window (Spacing between different buses on the same day)
    headway_window = Window.partitionBy(
        "naptan_id", 
        "line_name", 
        F.to_date("expected_arrival")
    ).orderBy("expected_arrival")
    
    df_silver = df_with_delays.withColumn(
        "previous_bus_arrival", F.lag("expected_arrival", 1).over(headway_window)
    ).withColumn(
        "headway_minutes",
        (F.col("expected_arrival").cast("long") - F.col("previous_bus_arrival").cast("long")) / 60
    )

    return df_silver



# ==========================================
# 3. RUNTIME EXECUTION
# ==========================================

def main():
    """     
    Silver layer cleans and calculates metrics.
    """    
    logger.info("Starting Bronze to Silver transformation pipeline...")
    
    # Load history; find new files
    processed_history = load_processed_files()
    files_to_process = find_new_bronze_files(Path(BRONZE_DIR), processed_history)
    
    if not files_to_process:
        logger.warning("No new bronze files found. Exiting.")
        return

    # transform bronze to silver  
    df_bronze = read_latest_bronze_batches(files_to_process)
    df_silver = transform_bronze_to_silver(df_bronze)
    
   
    columns_to_display = [
        "line_name", "station_name", "vehicle_id", "expected_arrival",
        F.round("delay_minutes", 2).alias("delay_min"),
        F.round("headway_minutes", 2).alias("headway_min")
    ]
    sample_station = df_silver.select("station_name").first()["station_name"]
    df_checkpoint = df_silver.select(columns_to_display).filter(F.col("station_name") == sample_station).orderBy("expected_arrival")
    logger.info("Check cleaned silver metrics")
    # df_checkpoint.show(50, truncate=False)
 
    # write to disk
    logger.info(f"Writing clean data to silver directory: {SILVER_DIR}")
    (
        df_silver.write
        .mode("append")            
        .partitionBy("line_name")  
        .parquet(str(SILVER_DIR))
    )
    
    logger.info("Updating processed file history")    
    logger.success("Success! Silver layer is ready.")


if __name__ == "__main__":
    main()