
# ingestion/producer.py -  ingestion TfL API - producer.py - Kafka topic
from datetime import datetime, timezone
import json
import time

from loguru import logger
from confluent_kafka import Producer
import requests


from pipeline_config import (
    KAFKA_BROKER,
    KAFKA_TOPIC_RAW,
    LOG_LEVEL,
    POLL_INTERVAL_SEC,
    TFL_URL,
)


# --- Logging ---
logger.remove()  # Removes default handler
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)

# --- Kafka delivery callback ---
# confluent-kafka is async, queues msgs and sends in the background
# when Kafka acknowledges/denies a msg, the function is called automatically . Without it, wouldn't know if something is dropped.
def _on_delivery(err, msg) -> None:
    if err:
        logger.error(f"Kafka delivery failed. Reason: {err}")

    else:
        logger.trace(
            f"Delivered | Topic={msg.topic()} "
            f"Partition={msg.partition()} "
            f"Offset={msg.offset()}"
        )


# --- TFL API ---
# def _build_headers() -> dict:
#     """Builds request header """
#     headers = {"Accept": "application/vnd.github.v3+json"}
#     if TFL_PRIMARY_TOKEN:
#         headers["Authorization"] = f"Bearer {TFL_PRIMARY_TOKEN}"
#     return headers


def format_time(iso_timestamp: str) -> str:
    dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    return dt.strftime("%H:%M:%S")


def print_arrivals(arrivals: list[dict], limit: int = 10) -> None:
    print(
        f"{'ID':>12}  "
        f"{'Vehicle':<10}  "
        f"{'Line':<5}  "
        f"{'Direction':<9}  "
        f"{'Stop':<35}  "
        f"{'ETA(s)':>7}  "
        f"{'Arrival':>8}"
    )
    print("-" * 100)

    for row in sorted(arrivals, key=lambda r: r["timeToStation"])[:limit]:
        print(
            f"{row['id']:>12}  "
            f"{row['vehicleId']:<10}  "
            f"{row['lineId']:<5}  "
            f"{row['direction']:<9}  "
            f"{row['stationName'][:35]:<35}  "
            f"{row['timeToStation']:>7}  "
            f"{format_time(row['expectedArrival']):>8}"
        )

def normalise_arrival(row: dict) -> dict:
    """Selects the fields required for the Bronze arrival event schema."""
    return {
        "source_prediction_id": row.get("id"),
        "vehicle_id": row.get("vehicleId"),
        "line_id": row.get("lineId"),
        "line_name": row.get("lineName"),
        "trip_id": row.get("tripId"),
        "naptan_id": row.get("naptanId"),
        "station_name": row.get("stationName"),
        "platform_name": row.get("platformName"),
        "direction": row.get("direction"),
        "destination_name": row.get("destinationName"),
        "time_to_station": row.get("timeToStation"),
        "expected_arrival": row.get("expectedArrival"),
        "api_timestamp": row.get("timestamp"),
        "mode_name": row.get("modeName"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

# --- TFL API ---
# def fetch_predictions(headers: dict) -> list[dict]:
def fetch_arrivals() -> list[dict]:

    """
    Retrieves live ETA prediction data for TfL bus line 46.
    Returns an empty list on error so the poll loop does not crash.
    """
    try:
        # response = requests.get(TFL_URL, headers=headers, timeout=10)
        response = requests.get(TFL_URL, timeout=10)

        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"TFL API request failed. Reason: {e}")
        return []



# --- Producer ---
def run_producer() -> None:
    """
    Main loop. Polls TFL every 30 seconds.
    Flow per poll cycle:
    fetch_events() produce() to Kafka -> sleep.
    """
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    # headers = _build_headers()

    logger.info(
        f"Producer started | Broker: {KAFKA_BROKER} | "
        f"Topic: {KAFKA_TOPIC_RAW} | Poll interval: {POLL_INTERVAL_SEC}s | "
        f"Source: TfL line 46 arrivals"
    )

    while True:
        arrivals = fetch_arrivals()
        logger.info(f"Fetched {len(arrivals)} arrivals from TfL")

        # print_arrivals(arrivals[:4])  debug
    
        for arrival in arrivals:
            record = normalise_arrival(arrival)

            # key represents a bus approaching the same stop on the same line across each api poll
            arrival_id = f"{record['line_id']}:{record['naptan_id']}:{record['vehicle_id']}"
            
            # produce() is non-blocking, it puts msg in an internal queue
            producer.produce(
                topic=KAFKA_TOPIC_RAW,
                key=arrival_id,
                value=json.dumps(record),
                callback=_on_delivery
            )
            producer.poll(0)         # poll(0) gives Kafka a chance to send what was put in the queue.

        producer.flush()

        logger.info(f"Published {len(arrivals)} arrivals to Kafka topic {KAFKA_TOPIC_RAW}")
        logger.info(f"Sleeping {POLL_INTERVAL_SEC}s until next poll")
        time.sleep(POLL_INTERVAL_SEC)



# --- Entrypoint -----------------------------------------------------------
if __name__ == "__main__":
    run_producer()