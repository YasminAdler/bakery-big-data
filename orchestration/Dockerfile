FROM apache/airflow:2.7.2

# Switch to root to install packages
USER root

# Install Docker CLI and make
RUN apt-get update && apt-get install -y \
    docker.io \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Keep running as root to access Docker socket
USER root 