from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, ArrayType
import os

# 1) Define the schema exactly matching your payload
jobSchema = StructType() \
    .add("jobId", StringType()) \
    .add("title", StringType()) \
    .add("company", StringType()) \
    .add("location", StringType()) \
    .add("skills", StringType()) \
    .add("applyUrl", StringType()) \
    .add("description", StringType()) \
    .add("requirements", ArrayType(StringType())) \
    .add("applyLink", StringType())

# 2) Bootstrap SparkSession with Kafka support

spark = SparkSession.builder \
    .appName("LinkedInJobIngest") \
    .getOrCreate()

# 3) Read from Kafka as a streaming source
df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", 
                 os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
         .option("subscribe", os.getenv("KAFKA_TOPIC", "jobs"))
         .option("startingOffsets", "earliest")
         .load()
)

# 4) The Kafka `value` is binary JSON—convert to string then parse
jsonDF = (
    df.selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), jobSchema).alias("data"))
      .select("data.*")
      .withColumn("ingest_ts", current_timestamp())
)

# 5) Write out bronze parquet files in append mode
query = (
    jsonDF.writeStream
          .format("parquet")
          .option("path", "/data/bronze/jobs")       # mounts to host via your volume
          .option("checkpointLocation", "/data/bronze/_checkpoints")
          .partitionBy("company")                    # e.g. partitioning
          .outputMode("append")
          .start()
)

query.awaitTermination()
