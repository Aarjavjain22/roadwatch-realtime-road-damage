"""Spark Structured Streaming aggregation for RoadWatch detection events."""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, to_timestamp, window
from pyspark.sql.types import DoubleType, StringType, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/roadwatch")
POSTGRES_USER = os.getenv("POSTGRES_USER", "roadwatch")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "roadwatch")
DET_TOPIC = "road_damage_events"

schema = (
    StructType()
    .add("event_version", StringType())
    .add("event_id", StringType())
    .add("trace_id", StringType())
    .add("frame_id", StringType())
    .add("timestamp", StringType())
    .add("road_segment", StringType())
    .add("damage_type", StringType())
    .add("confidence", DoubleType())
    .add("severity", StringType())
)


def write_to_postgres(batch_df, batch_id):
    (batch_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "segment_window_agg")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())


def main():
    spark = SparkSession.builder.appName("RoadWatchStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", DET_TOPIC)
           .option("startingOffsets", "earliest")
           .load())

    events = (raw.select(from_json(col("value").cast("string"), schema).alias("e"))
              .select("e.*")
              .withColumn("event_time", to_timestamp(col("timestamp"))))

    agg = (events
           .withWatermark("event_time", "20 seconds")
           .groupBy(window(col("event_time"), "10 seconds"), col("road_segment"), col("damage_type"))
           .agg(count("*").alias("event_count"), avg("confidence").alias("avg_confidence"))
           .select(
               col("window.start").alias("window_start"),
               col("window.end").alias("window_end"),
               col("road_segment"), col("damage_type"),
               col("event_count"), col("avg_confidence")))

    query = (agg.writeStream
             .foreachBatch(write_to_postgres)
             .outputMode("append")
             .option("checkpointLocation", "/checkpoint")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
