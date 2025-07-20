FROM apache/airflow:2.7.2

USER root

RUN apt-get update && apt-get install -y \
    docker.io \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER root 