

# Real Time traffic
## Project goal

Track and analyse London bus arrival stream data as an end-to-end pipeline. 

Live arrival data via Kafka, 
Validating and storing dat in parquet
Data transformation pyspark and dbt
producing analytical views for service reliability, prediction drift, headway gaps, route congestion, and feed quality.
Goal:
Investigate reddit user complaint about bus 46 route.



Version 2
Streamlit dashboard drilldown for route-level and stop-level trends.
Check trends connected to wait times, bus bunching, stale predictions, and 


## Pipeline Construction
Pipeline adopts a medallion (bronze, silver, gold) structure.

### Bronze
Kafka ingests realtime bus arrivals data stream -> parquet

### Silver
...



## commands

### Image setup
handshake apache kafka image
```docker pull apache/kafka:latest```

Note: Verify image is Local and Docker is tracking it by listing available images:
```docker images```
Mac tfl % docker images
REPOSITORY     TAG       IMAGE ID       CREATED        SIZE
apache/kafka   latest    ef33ec581463   4 months ago   439MB


### Container (pipeline) setup
start kafka broker using docker-compose.yml, 
```docker compose up -d```

Note: --build only needed when building a custom Dockerfile inside the compose file. 
In this case, pulling a pre-built image (apache/kafka:latest), omit --build 
-d runs container in "detached" mode, prevents impact on terminal.

```docker ps```
CONTAINER ID   IMAGE                 COMMAND                  CREATED          STATUS         PORTS                                              NAMES
331be8322222   apache/kafka:latest   "/__cacert_entrypoin…"   10 seconds ago   Up 9 seconds   0.0.0.0:9999->9999/tcp, 0.0.0.0:99999->99999/tcp   tfl-kafka
