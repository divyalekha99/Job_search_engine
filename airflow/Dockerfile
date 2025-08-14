# 1) Base image
FROM apache/airflow:3.0.2

# 2) As root, install system deps
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         docker.io \
         docker-compose-plugin \
    && ln -s /usr/libexec/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 3) Switch to airflow user for Python installs
USER airflow
ENV PATH="/home/airflow/.local/bin:$PATH"

# 4) Install the Spark provider into Airflow
RUN pip install --no-cache-dir --user \
      apache-airflow-providers-apache-spark==5.3.1 \
      apache-airflow-providers-docker

# 5) Ensure airflow user for all later operations
USER airflow