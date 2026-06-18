# ADR 002: Choosing Kafka + Parquet for TfL Pipeline

## Context
Capture live London bus arrivals times telemetry to analyze systemic route delays. The TfL API updates every 30 seconds, generating high-velocity event bursts.

## Decision
Use an Apache Kafka broker to buffer raw JSON API payloads combined with a consumer.py that flushes hive-partitioned parquet files into a bronze storage layer.

## Consequences
- resilient to network spikes.
- efficient historical analysis via hive partition pruning.
- avoids data loss during downstream warehouse outages.