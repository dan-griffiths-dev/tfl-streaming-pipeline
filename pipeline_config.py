# Config.py

from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"


# --- TFL API ---
TFL_TOKEN = os.getenv("TFL_TOKEN")  # 
TFL_LINE_ID = os.getenv("TFL_LINE_ID", "46")        # filtered to Bus Route 46 while prototyping
TFL_URL = f"https://api.tfl.gov.uk/line/{TFL_LINE_ID}/arrivals"
POLL_INTERVAL_SEC = 35              # tfl streams at 30 second intervals


BRONZE_DIR = DATA_DIR / "bronze" / "arrivals"
SILVER_DIR = DATA_DIR / "silver" / "arrivals"
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


# --- Partitioning ---
# Use hive style folder structure in data/bronze year=/month=/day=
# hive is suitable for Pyspark 
DATE_PARTITION_FORMAT = "year={year}/month={month:02d}/day={day:02d}"





# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = ROOT_DIR / "logs"


