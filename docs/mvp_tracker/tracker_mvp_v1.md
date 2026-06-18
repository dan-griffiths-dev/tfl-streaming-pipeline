Session tracking notes for MvP v1
COMPLETED

6/06/2026
Setup entire repo structure         Done
Add all needed deps for MvP 1       Done
Configure docker-compose.yml image  Done



11/06/2026
Write pipeline_config.py in repo    Done
Write kafka_producer.py in repo     Done
Write kafka_consumer.py in repo     Done

Write small docs for todays session (config, kafka_producer and kafka_consumer)
fetch bus arrivals from tfl -> parquet DONE
Bronze layer complete



To do:


Add visuals in form of .mmd diagrams to show flow with KRaft instead of Zookeeper for kafka
Configure Dockerfile + docker-compose.yml file

BRONZE TO SILVER
Write bronze_to_silver.py script.
Write what bronze_to_silver.py script does in docs/file_docs/bronze_to_silver.md
Idempotency check in bronze_to_silver.py. Last thing open on ROADMAP.md for MvP V1.
Testing
Write unit tests for _is_valid() and _flatten() functions in bronze_to_silver.py script.


README.md structure for MvP v1
