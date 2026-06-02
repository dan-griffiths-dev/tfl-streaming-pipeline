# Config.py

from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"


# --- TFL API ---
TFL_TOKEN = os.getenv("TFL_TOKEN")  # 
TFL_URL = "https://api.tfl.gov.uk/line/46/arrivals"     # filtered to Bus Route 46 while prototyping
POLL_INTERVAL_SEC = 35              # tfl streams at 30 second intervals


BRONZE_DIR = DATA_DIR / "bronze" / "events"
SILVER_DIR = DATA_DIR / "silver" / "events"
GOLD_DIR = DATA_DIR / "gold"
# Checkpoint path
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
BRONZE_SILVER_CHECKPOINT = CHECKPOINT_DIR / "bronze_to_silver.json"



# --- Kafka ---
# producer.py and consumer.py import from here, independent of each other
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC_RAW = "tfl.bus.arrivals.raw"  # Bronze. Raw data
KAFKA_TOPIC_DLQ = "?????-????"  # Dead letter queue
KAFKA_GROUP_ID = "tfl-lake-?????"



# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = ROOT_DIR / "logs"



# constructing a data lake for streamed data to more clearly understand the data lifecycle, kofka engineering, and the medallion layering architecture.
    
# The project ingests and analyses TFL bus arrival prediction data with the intentoin for the pipeline to deliver streamed timing and bus line data issued every 30 seconds. Th eMVP will foucs on a single bus line and seek to investigate complaitns made about the bus line lateness on reddit. The project demonstrates a complete end-to-end pipeline which streams raw data via kafka into validated historical parquet storage.  MVP 2 will focus on reporitng insights in streamlit/powerbi dsashbaords to offer self serving insight reporting using analytical views dbt/PySpark.

# .github
# data
# dbt
# docs
# ingestion
# __init__.py
# consumer.py
# producer.py
# orchestration
# scripts
# serving
# tests
# transforms
# .dockerignore
# .env.example
# .gitignore
# .python-version
# Dockerfile
# Dockerfile.dbt
# Dockerfile.spark
# LICENSE
# README.md
# ROADMAP.md
# config.py
# dependencies_to_add.md
# docker-compose.yml
# pyproject.toml
# uv.lock