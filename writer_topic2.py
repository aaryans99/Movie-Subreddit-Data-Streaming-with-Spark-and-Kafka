from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode, count, to_json, struct
from pyspark.sql.functions import pandas_udf
import pandas as pd
import spacy

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RedditNamedEntities") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Load Kafka stream
try:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "topic1") \
        .load()
except Exception as e:
    print(f"Error reading Kafka stream: {e}")
    spark.stop()
    exit(1)

# Extract comments as strings
comments = df.selectExpr("CAST(value AS STRING)")

# Load spaCy model for named entity recognition
try:
    nlp = spacy.load("en_core_web_sm")
except Exception as e:
    print(f"Error loading spaCy model: {e}")
    spark.stop()
    exit(1)

# Define a pandas UDF to extract named entities
@pandas_udf("array<string>")
def extract_entities(texts: pd.Series) -> pd.Series:
    try:
        return texts.apply(lambda text: [ent.text for ent in nlp(text).ents])
    except Exception as e:
        print(f"Error during entity extraction: {e}")
        return pd.Series([[]] * len(texts))

# Apply UDF to extract entities
entities_df = comments.withColumn("named_entity", extract_entities(col("value")))

# Flatten entities and count occurrences
exploded_entities = entities_df.select(explode(col("named_entity")).alias("entity"))
entity_counts = exploded_entities.groupBy("entity").count()

# Prepare data for Kafka output
kafka_output_df = entity_counts.selectExpr(
    "CAST(entity AS STRING) AS key", 
    "to_json(struct(*)) AS value"
)

# Write processed data back to Kafka
try:
    kafka_output_query = kafka_output_df \
        .writeStream \
        .outputMode("update") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "topic2") \
        .option("checkpointLocation", "/home/legion/structured_kafka_project/spark_checkpoints") \
        .start()
    kafka_output_query.awaitTermination()
except Exception as e:
    print(f"Error writing to Kafka: {e}")
finally:
    # Stop the Spark session gracefully
    spark.stop()
