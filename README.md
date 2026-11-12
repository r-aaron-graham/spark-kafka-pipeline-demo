# Spark Kafka Pipeline Demo

## Overview
A demo of a PySpark Structured Streaming job that reads JSON messages from Kafka, performs simple transformations, and writes to Parquet.

## Run the local pipeline

Start a local Kafka broker at `localhost:9092` and create the `demo-topic`
topic. Install the Python dependencies. Run the consumer with Spark's
`spark-sql-kafka-0-10` connector on its classpath, using the artifact that
matches your Spark and Scala versions.

Start the consumer, then run `python src/producer.py` in another terminal.
The producer sends synthetic events with an ID, numeric value, and Unix
timestamp. The consumer parses them and writes Parquet under
`output/events`, with progress stored under `output/checkpoint`.

The checked-in `config.yaml` uses the producer's default broker and topic.
The consumer resolves the configuration from the repository directory.
