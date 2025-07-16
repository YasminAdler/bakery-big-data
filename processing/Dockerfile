FROM bitnami/spark:3.4.1

USER root

# Install additional Python packages
RUN pip install --no-cache-dir \
    pyiceberg==0.4.0 \
    pyspark==3.4.1 \
    kafka-python==2.0.2 \
    boto3==1.28.62 \
    pandas==2.0.3 \
    pyarrow==12.0.1

# Download necessary JARs for Iceberg, Kafka, and S3
RUN cd /opt/bitnami/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.3.1/iceberg-spark-runtime-3.4_2.12-1.3.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Set up Spark defaults for Iceberg
COPY config/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

USER 1001 