# Movie-Subreddit-Data-Streaming-with-Spark-and-Kafka
This guide walks you through setting up a data pipeline that captures movies subreddit data, processes it, and visualizes it using various tools.

## Prerequisites
Before starting, make sure you have all required tools installed (Kafka, Zookeeper, Python, PySpark, Elasticsearch, Kibana, and Logstash).

## Step 1: Set Up Message Queuing Infrastructure
First, we need to get our message queue system running:

1. Launch Zookeeper (Kafka's coordination service):
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```

2. Start the Kafka message broker (in a new terminal):
   ```bash
   bin/kafka-server-start.sh config/server.properties
   ```

## Step 2: Start Data Collection and Processing

3. Begin collecting movies subreddit data (in a new terminal):
   ```bash
   python structured_kafka.py
   ```
   This script will stream data from Reddit to Kafka topic1.

4. Launch the data processing script:
   ```bash
   python writer_topic2.py
   ```
   This notebook processes incoming data and writes entity counts to Kafka topic2.

## Step 3: Set Up the Visualization Stack

5. Start Elasticsearch (the search engine):
   ```bash
   bin/elasticsearch
   ```

6. Launch Kibana (the visualization interface):
   ```bash
   bin/kibana
   ```

7. Configure data ingestion with Logstash:
   ```bash
   bin/logstash -f logstash.conf
   ```
   This pulls data from Kafka topic2 into Elasticsearch.

## Final Step: Create Visualizations

8. Access Kibana through your web browser to create custom visualizations of your movies subreddit data.

**Note:** Run each command in a separate terminal window and keep them running throughout your analysis session.
