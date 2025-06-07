
# Create a directory to store the JARs if it doesn't exist
mkdir -p jars

# --- Define JAR versions ---
SPARK_VERSION="3.5.0"
SCALA_VERSION="2.12"
ICEBERG_VERSION="1.5.0" # A recent version compatible with Spark 3.5
AWS_SDK_VERSION="1.12.367"
HADOOP_AWS_VERSION="3.3.4"

# --- JAR URLs from Maven Central ---
ICEBERG_JAR_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"
AWS_SDK_JAR_URL="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"
HADOOP_AWS_JAR_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
KAFKA_CONNECTOR_URL="https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"


# --- Download the JARs ---
echo "Downloading Apache Iceberg Spark Runtime..."
wget -P ./jars/ ${ICEBERG_JAR_URL}

echo "Downloading AWS SDK Bundle for S3 access..."
wget -P ./jars/ ${AWS_SDK_JAR_URL}

echo "Downloading Hadoop AWS for S3 filesystem..."
wget -P ./jars/ ${HADOOP_AWS_JAR_URL}

echo "Downloading Spark-SQL-Kafka Connector..."
wget -P ./jars/ ${KAFKA_CONNECTOR_URL}

echo "------------------------------------"
echo "All JAR files downloaded successfully into the 'processing/jars/' directory."
ls -la ./jars/
echo "------------------------------------"