

# Real Time traffic
## Project goal

To track and analyse operational patterns in TfL real-time bus arrival stream data. The project demonstrates a complete end-to-end data engineering pipeline: ingesting live arrival data into Kafka, validating and storing historical snapshots as Parquet, transforming the data with PySpark/dbt, and producing analytical views for service reliability, prediction drift, headway gaps, route congestion, and feed quality.

Version 2
Add a Streamlit dashboard to explore route-level and stop-level trends, including abnormal wait times, bus bunching, stale predictions, and complaint validation for specific routes such as the 46 bus.