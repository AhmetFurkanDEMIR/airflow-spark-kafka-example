from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DataProcessing").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5").getOrCreate()

kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "example_data"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Perform your data processing here
processed_df = df.select(col("value"))

query = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
