# FROM python:3.10-slim

# WORKDIR /opt/app

# # install Python deps
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # copy your Spark job
# COPY spark_ingest.py .

# # when container starts, launch spark-submit
# CMD ["spark-submit",
#      "--master", "local[*]",
#      "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
#      "spark_ingest.py"]
# spark/Dockerfile
FROM openjdk:17-slim

# install Python & shell deps
RUN apt-get update \
 && apt-get install -y python3-pip curl \
 && rm -rf /var/lib/apt/lists/*

# install PySpark + Kafka connector
RUN pip3 install --no-cache-dir pyspark kafka-python

WORKDIR /app
COPY spark_ingest.py .

# drop any line breaks in the JSON-array form of CMD
# CMD ["spark-submit","--master","local[*]","--packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0","spark_ingest.py"]
