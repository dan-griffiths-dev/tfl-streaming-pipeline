import json
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from loguru import logger


from pipeline_config import (
    BRONZE_DIR,
    DATE_PARTITION_FORMAT,
    KAFKA_BROKER,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC_RAW,
    LOG_LEVEL,
)


# --- Logging ---
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)

# --- Constants ---
# Batch size controls how often write to disk. 100x events OR 60 sec. 
# Whichever happens first triggers the write.
# Too small batch = too many small Parquet files (inefficient for PySpark later).
# Too large batch = too long between writes (more data to lose in case of crash).
BATCH_SIZE = 100
BATCH_TIMEOUT_SEC = 60

# Group events by partition date

# --- Parquet writing ---
def _write_batch_to_bronze(batch: list[dict]) -> None:
    """
    Writes a batch of events as Parquet to represent the Bronze layer.
    Trade off: parquet versus

    Each event gets its own row. The path is built from the event's created_at
    so that the data ends up in the correct year/month/day partition automatically.

    Grouping the batch by date before writing, a batch can contain
    events from midnight onwards, e.g. 23:59 and 00:01, which go to
    different day folders.
    """
    # groups events by partition date
    partitions: dict[str, list[dict]] = {}

    for arrival in batch:
        
        created_at = arrival.get("created_at", "")
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            
            dt = datetime.now(timezone.utc)
            logger.warning(
                f"Invalid created_at for arrival {arrival.get('id')} using current UTC time"
            )

        # --- Serialize payload to JSON string ---
        # PyArrow infers schema from the batch when it runs from_pylist().
        # Payload varies structurally per event type, a PushEvent has
        # "commits" which is a list of objects, a WatchEvent does not have it at all.
        # which causes the "commits" to silently disappear from the Bronze file..
        # Solution: serialize payload to a JSON str BEFORE PyArrow even
        # sees it. A string always has consistent schema it's just text.
        # In _flatten() deserializes the string back with json.loads().

        arrival_copy = arrival.copy()
        if "payload" in arrival_copy:
            arrival_copy["payload"] = json.dumps(arrival_copy["payload"])

        partition_key = DATE_PARTITION_FORMAT.format(
            year=dt.year,
            month=dt.month,
            day=dt.day,
        )
        partitions.setdefault(partition_key, []).append(arrival_copy)

    # Write each partition to its own folder
    for partition_key, arrivals in partitions.items():
        output_path = BRONZE_DIR / partition_key
        output_path.mkdir(parents=True, exist_ok=True)

        # Convert the list of dicts to a PyArrow table. 
        # PyArrow arranges schema automatically from the data, no need to 
        # define columns manually in Bronze. Raw data is saved as is.
        table = pa.Table.from_pylist(arrivals)

        # Build a unique filename based on timestamp to avoid overwriting. 
        # If consumer restarts (after crash) it creates a new file next to the old one.
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        output_file = output_path / f"part-{timestamp}.parquet"

        pq.write_table(table, output_file, compression="snappy")

        logger.info(f"Wrote {len(arrivals)} arrivals -> {output_file}")


# --- Consumer ---
def run_consumer() -> None:
    """
    Main loop. Consumes arrivals from Kafka and writes bronze layer to parquet.

    Flow:
    poll() from kafka -> 
        collect in batch -> 
            batch full/timeout -> 
                write parquet -> 
                    commit offset

    Note order - write to disk then commit to kafka.
    If script crashes after writing but before committing, possible to rerun and write
    the again. If committed then crash occurs, data lost permanently.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": KAFKA_GROUP_ID,
            # "earliest" - if there is no saved offset for this consumer group, 
            #  start from the oldest message in the topic.
            # "latest" - omit message history, start from the latest message.
            "auto.offset.reset": "earliest",
            # commits manually after disk write
            # auto.commit=true would have committed on time, regardless of whether it was written to disk.
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([KAFKA_TOPIC_RAW])
    logger.info(f"Consumer started | Broker: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC_RAW}")

    batch: list[dict] = []
    last_flush = datetime.now(timezone.utc)

    try:
        while True:
            # poll(1.0) = wait max 1s for new message.
            # Returns None if times out without a message.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No data came in, check if timeout flush is needed
                # TODO
                pass
            elif msg.error():
               # PARTITION_EOF = end of a partition. New messages still enter.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        f"End of partition reached: {msg.topic()} [{msg.partition()}]"
                    )
                else:
                    if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logger.warning("Topic not available yet, retrying in 5 sec..")
                        time.sleep(30)
                    else:
                        raise KafkaException(msg.error())
            else:
                try:
                    arrival = json.loads(msg.value().decode("utf-8"))
                    batch.append(arrival)
                
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(f"Failed to deserialize message: {e}")

            # --- Flush-logic ----------------------

            now = datetime.now(timezone.utc)
            elapsed = (now - last_flush).total_seconds()
            batch_full = len(batch) >= BATCH_SIZE
            timeout_reached = elapsed >= BATCH_TIMEOUT_SEC and len(batch) > 0

            if batch_full or timeout_reached:
                reason = "batch_full" if batch_full else "timeout"
                logger.info(f"Flushing batch | reason={reason} | size={len(batch)}")
                
                # STEP 1: Write to disk (critical step)
                _write_batch_to_bronze(batch)

                # STEP 2: Commit offset to Kafka (only when data is safe)
                consumer.commit(asynchronous=False)

                batch.clear()
                last_flush = now

    except KeyboardInterrupt:
        # Ctrl+C, write what is left in the batch before it closes
        logger.info("Shutdown signal received")
        
        if batch:
            logger.info(f"Flushing remaining {len(batch)} events before exit")
            _write_batch_to_bronze(batch)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        logger.info("Consumer closed cleanly")




# --- Entrypoint ---
if __name__ == "__main__":
    run_consumer()