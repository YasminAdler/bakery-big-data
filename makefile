# Bakery Data Pipeline - Makefile
# For best results, run from Git Bash or WSL on Windows

.PHONY: help start stop build clean init status logs load-bronze csv-producer

# Compose files
STREAMING_COMPOSE = streaming/docker-compose.yml
PROCESSING_COMPOSE = processing/docker-compose.yml
ORCHESTRATION_COMPOSE = orchestration/docker-compose.yml

# Use docker-compose if available, else fallback to 'docker compose'
DOCKER_COMPOSE = $(shell command -v docker-compose 2>/dev/null || echo "docker compose")

help:
	@echo "Available targets:"
	@echo "  make start     - Build and start all services, initialize pipeline"
	@echo "  make stop      - Stop all services"
	@echo "  make build     - Build all Docker images"
	@echo "  make clean     - Remove all containers, volumes, and network"
	@echo "  make status    - Show status of all services"
	@echo "  make logs      - Show logs for all services"

build:
	cd streaming && "$(DOCKER_COMPOSE)" build
	cd processing && "$(DOCKER_COMPOSE)" build
	cd orchestration && "$(DOCKER_COMPOSE)" build || true

start: build
	@echo "Starting Docker network..."
	@docker network create bakery-network 2>/dev/null || true
	@echo "Starting streaming services..."
	cd streaming && "$(DOCKER_COMPOSE)" up -d
	@echo "Starting processing services..."
	cd processing && "$(DOCKER_COMPOSE)" up -d
	@echo "Starting orchestration services..."
	cd orchestration && "$(DOCKER_COMPOSE)" up -d
	@echo "Waiting for services to initialize..."
	@sleep 10
	@$(MAKE) init
	@$(MAKE) status

init:
	@echo "Initializing MinIO buckets..."
	@docker exec minio mc alias set myminio http://minio:9000 minioadmin minioadmin 2>/dev/null || true
	@docker exec minio mc mb myminio/bronze --ignore-existing 2>/dev/null || true
	@docker exec minio mc mb myminio/silver --ignore-existing 2>/dev/null || true
	@docker exec minio mc mb myminio/gold --ignore-existing 2>/dev/null || true
	@docker exec minio mc mb myminio/iceberg-warehouse --ignore-existing 2>/dev/null || true
	@echo "Creating Kafka topics..."
	@docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic sales-events --partitions 3 --replication-factor 1 2>/dev/null || true
	@docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic equipment-metrics --partitions 3 --replication-factor 1 2>/dev/null || true
	@docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic inventory-updates --partitions 3 --replication-factor 1 2>/dev/null || true
	@echo "Copying Spark jobs to containers..."
	@docker cp processing/jobs/. spark-submit:/opt/spark-apps/ 2>/dev/null || true
	@docker cp processing/config/. spark-submit:/opt/spark/conf/ 2>/dev/null || true
	@echo "Copying Airflow DAGs and plugins..."
	@docker cp orchestration/dags/. airflow-webserver:/opt/airflow/dags/ 2>/dev/null || true
	@docker cp orchestration/plugins/. airflow-webserver:/opt/airflow/plugins/ 2>/dev/null || true
	@docker cp orchestration/dags/. airflow-scheduler:/opt/airflow/dags/ 2>/dev/null || true
	@docker cp orchestration/plugins/. airflow-scheduler:/opt/airflow/plugins/ 2>/dev/null || true
	@echo "Initializing Iceberg tables..."
	@docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.local.type=hadoop \
		--conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minioadmin \
		--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.local.dir=/tmp \
		--conf hadoop.tmp.dir=/tmp \
		/opt/spark-apps/init_iceberg_tables.py 2>/dev/null || true

stop:
	cd orchestration && "$(DOCKER_COMPOSE)" down
	cd processing && "$(DOCKER_COMPOSE)" down
	cd streaming && "$(DOCKER_COMPOSE)" down
	@docker network rm bakery-network 2>/dev/null || true

clean: stop
	cd orchestration && "$(DOCKER_COMPOSE)" down -v --remove-orphans
	cd processing && "$(DOCKER_COMPOSE)" down -v --remove-orphans
	cd streaming && "$(DOCKER_COMPOSE)" down -v --remove-orphans
	@docker system prune -f

status:
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(kafka|spark|airflow|minio|zookeeper)" || echo "No services running"
	@docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "Kafka not available"
	@docker exec minio mc ls myminio/ 2>/dev/null || echo "MinIO not available"

logs:
	@docker logs --tail 10 kafka-broker 2>/dev/null || echo "Kafka not running"
	@docker logs --tail 10 spark-master 2>/dev/null || echo "Spark not running"
	@docker logs --tail 10 airflow-webserver 2>/dev/null || echo "Airflow not running" 

# -------------------------------------------------------------------
# Convenience / demo targets restored
# -------------------------------------------------------------------

producers:
	@echo "Starting Kafka data producers (sales, equipment, inventory)..."
	@docker exec -d kafka-producer python /app/generate_sales_events.py
	@docker exec -d kafka-producer python /app/generate_equipment_metrics.py
	@docker exec -d kafka-producer python /app/generate_inventory_updates.py

streaming:
	@echo "Launching Spark streaming job (stream_to_bronze.py)..."
	@docker exec -d spark-submit spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.local.type=hadoop \
		--conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minioadmin \
		--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.local.dir=/tmp \
		--conf hadoop.tmp.dir=/tmp \
		/opt/spark-apps/stream_to_bronze.py

batch-etl:
	@echo "Running batch ETL (Bronze → Silver → Gold)..."
	@docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.local.type=hadoop \
		--conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minioadmin \
		--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.local.dir=/tmp \
		--conf hadoop.tmp.dir=/tmp \
		--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
		--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
		/opt/spark-apps/bronze_to_silver.py
	@docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.local.type=hadoop \
		--conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minioadmin \
		--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.local.dir=/tmp \
		--conf hadoop.tmp.dir=/tmp \
		--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
		--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
		/opt/spark-apps/silver_to_gold.py

health:
	@echo "Running quick health checks..."
	@docker exec kafka-broker kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && echo "Kafka ✅" || echo "Kafka ❌"
	@curl -s http://localhost:8080 >/dev/null 2>&1 && echo "Spark ✅" || echo "Spark ❌"
	@curl -s http://localhost:8081/health >/dev/null 2>&1 && echo "Airflow ✅" || echo "Airflow ❌"
	@docker exec minio mc admin info myminio >/dev/null 2>&1 && echo "MinIO ✅" || echo "MinIO ❌"

show-data:
	@echo "Showing record counts in key tables..."
	@docker exec spark-submit spark-sql \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,software.amazon.awssdk:bundle:2.20.18,org.apache.hadoop:hadoop-aws:3.3.4 \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.local.type=hadoop \
		--conf spark.sql.catalog.local.warehouse=s3a://iceberg-warehouse/ \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minioadmin \
		--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.local.dir=/tmp \
		--conf hadoop.tmp.dir=/tmp \
		--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
		--conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
		-e "SELECT 'Bronze Sales Events' AS table_name, COUNT(*) AS cnt FROM local.bronze.sales_events;" \
		-e "SELECT 'Silver Sales'        AS table_name, COUNT(*) AS cnt FROM local.silver.sales;" \
		-e "SELECT 'Gold Fact Sales'    AS table_name, COUNT(*) AS cnt FROM local.gold.fact_sales;"

# -------------------------------------------------------------------
# One-time CSV ingest target
# -------------------------------------------------------------------

load-bronze:
	@echo "Loading bronze_combined.csv ..."
	@docker exec spark-submit spark-submit \
		--master local[*] \
		/opt/spark-apps/load_bronze_from_csv.py
	@echo "✓ CSV loaded. Run 'make batch-etl' next."

# Demo: full pipeline in one go

demo:
	@echo "Running full demo (start → producers → streaming → batch ETL)..."
	@$(MAKE) start
	@sleep 10
	@$(MAKE) producers
	@echo "Generating data for 30 seconds..."
	@sleep 30
	@$(MAKE) streaming
	@sleep 20
	@$(MAKE) batch-etl
	@$(MAKE) status
	@echo "Demo completed — check MinIO, Spark UI, and Airflow!" 

csv-producer:
	@echo "Streaming bronze_combined.csv through Kafka topics ..."
	@docker exec -d kafka-producer python /app/csv_to_kafka.py /app/data/bronze_combined.csv
	@echo "✓ CSV producer running in background (kafka-producer container)" 