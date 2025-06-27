# Makefile for the Bakery Data Engineering Project
# This Makefile is designed for the multi-directory docker-compose setup.

# Define the compose files to use.
# Docker Compose will merge these files, with later files overriding earlier ones if there are conflicts.
COMPOSE_FILES = \
    -f docker-compose.yml \
    -f processing/docker-compose.yml \
    -f streaming/docker-compose.yml \
    -f orchestration/docker-compose.yml

# Docker command - use 'docker compose' for v2, 'docker-compose' for v1
DOCKER_COMPOSE = docker compose
# Define producer services
PRODUCERS = pos-producer iot-producer inventory-producer feedback-producer batch-producer

# Use .PHONY to ensure these targets run even if files with the same name exist
.PHONY: help start stop down init logs ps restart status clean-logs airflow-ui kafka-ui start-producers stop-producers restart-producers logs-producers

# Default command: show help message
help:
	@echo "------------------------------------------------------------------"
	@echo " Bakery Data Engineering Project - Command Menu"
	@echo "------------------------------------------------------------------"
	@echo "Usage: make [command]"
	@echo ""
	@echo "Commands:"
	@echo "  help              Show this help message."
	@echo "  start             Start all services from all modules in the background."
	@echo "  init              Initialize the system by creating Kafka topics."
	@echo "  stop              Stop all running services without removing data."
	@echo "  down              Stop all services and remove containers, networks, and volumes."
	@echo "  restart           Restart all services (stop then start)."
	@echo "  logs              Follow the logs of all running services."
	@echo "  ps                List all running containers for this project."
	@echo "  status            Show detailed status of all services."
	@echo "  airflow-ui        Open Airflow UI in browser (http://localhost:8080)."
	@echo "  kafka-ui          Open Kafka UI in browser (http://localhost:8081)."
	@echo "  clean-logs        Clean up Airflow logs directory."
	@echo "  check-buckets     List MinIO buckets."
	@echo ""
	@echo "Producer Commands:"
	@echo "  start-producers   Start all data producers."
	@echo "  stop-producers    Stop all data producers."
	@echo "  restart-producers Restart all data producers."
	@echo "  logs-producers    Show logs for all producers."
	@echo "------------------------------------------------------------------"

# Start all services defined in the compose files
start:
	@echo "[START] Starting all bakery project services..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) up -d --remove-orphans
	@echo "[SUCCESS] All services started successfully!"
	@echo "Access points:"
	@echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "  - Kafka UI: http://localhost:8081"
	@echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  - Spark Master: http://localhost:8082"
	@echo ""
	@echo "Producers running:"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) ps $(PRODUCERS)

# Stop running services
stop:
	@echo "[STOP] Stopping all services..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) stop
	@echo "[SUCCESS] All services stopped."

# Stop and remove all containers, networks, and data volumes for a clean restart
down:
	@echo "[TEARDOWN] Tearing down the entire environment..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) down --remove-orphans -v
	@echo "[SUCCESS] Environment cleaned up."

# Initialize the environment after services are started
init:
	@echo "[INIT] Initializing the system..."
	@echo "=> Creating MinIO buckets..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) up -d create-buckets || echo "Bucket creation service may have already run"
	@echo "=> Waiting 15 seconds for Kafka to be fully ready..."
	@sleep 15
	@echo "=> Creating Kafka topics..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T kafka kafka-topics --create --topic bakery-pos-events --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 --if-not-exists || echo "Topic bakery-pos-events already exists"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T kafka kafka-topics --create --topic bakery-iot-events --bootstrap-server kafka:9092 --partitions 2 --replication-factor 1 --if-not-exists || echo "Topic bakery-iot-events already exists"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T kafka kafka-topics --create --topic bakery-inventory-updates --bootstrap-server kafka:9092 --partitions 2 --replication-factor 1 --if-not-exists || echo "Topic bakery-inventory-updates already exists"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T kafka kafka-topics --create --topic bakery-customer-feedback --bootstrap-server kafka:9092 --partitions 2 --replication-factor 1 --if-not-exists || echo "Topic bakery-customer-feedback already exists"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T kafka kafka-topics --create --topic bakery-promotions --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --if-not-exists || echo "Topic bakery-promotions already exists"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T kafka kafka-topics --create --topic bakery-weather-data --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --if-not-exists || echo "Topic bakery-weather-data already exists"
	@echo "[SUCCESS] All Kafka topics created."
	@echo "=> Listing Kafka topics:"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T kafka kafka-topics --list --bootstrap-server kafka:9092
	@echo "=> MinIO buckets and Kafka topics are ready!"
	@echo "=> Note: Iceberg table creation is handled by the initial run of the Airflow DAGs."

# Follow logs from all services
logs:
	@echo "[LOGS] Tailing logs for all services... (Press Ctrl+C to stop)"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) logs -f --tail=100

# List running containers for this project
ps:
	@echo "[STATUS] Listing running containers..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) ps

# Restart all services
restart: stop start
	@echo "[SUCCESS] All services restarted."

# Show detailed status
status:
	@echo "[STATUS] Checking service health..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) ps
	@echo ""
	@echo "[KAFKA] Topics list:"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T kafka kafka-topics --list --bootstrap-server kafka:9092 2>/dev/null || echo "Kafka not ready"
	@echo ""
	@echo "[PRODUCERS] Status:"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) ps $(PRODUCERS) | grep -E "pos-producer|iot-producer|inventory-producer|feedback-producer|batch-producer" || echo "No producers running"

# Producer-specific commands
start-producers:
	@echo "[PRODUCERS] Starting all data producers..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) up -d $(PRODUCERS)
	@echo "[SUCCESS] All producers started."

stop-producers:
	@echo "[PRODUCERS] Stopping all data producers..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) stop $(PRODUCERS)
	@echo "[SUCCESS] All producers stopped."

restart-producers:
	@echo "[PRODUCERS] Restarting all data producers..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) restart $(PRODUCERS)
	@echo "[SUCCESS] All producers restarted."

logs-producers:
	@echo "[LOGS] Showing logs for all producers... (Press Ctrl+C to stop)"
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) logs -f --tail=50 $(PRODUCERS)

# Open Airflow UI
airflow-ui:
	@echo "[INFO] Opening Airflow UI..."
	@start http://localhost:8080 || open http://localhost:8080 || xdg-open http://localhost:8080

# Open Kafka UI
kafka-ui:
	@echo "[INFO] Opening Kafka UI..."
	@start http://localhost:8081 || open http://localhost:8081 || xdg-open http://localhost:8081

# Clean logs
clean-logs:
	@echo "[CLEAN] Removing old Airflow logs..."
	@rm -rf orchestration/logs/*
	@echo "[SUCCESS] Logs cleaned."

# View logs for specific service
logs-%:
	@echo "[LOGS] Showing logs for $*..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) logs -f --tail=100 $*

# Build images (useful for rebuilding producer images)
build:
	@echo "[BUILD] Building all Docker images..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) build
	@echo "[SUCCESS] All images built."

# Build only producer images
build-producers:
	@echo "[BUILD] Building producer images..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) build $(PRODUCERS)
	@echo "[SUCCESS] Producer images built."

# Check MinIO buckets
check-buckets:
	@echo "[MINIO] Checking MinIO buckets..."
	@$(DOCKER_COMPOSE) $(COMPOSE_FILES) exec -T minio sh -c "mc alias set myminio http://localhost:9000 minioadmin minioadmin && mc ls myminio/" || echo "MinIO not ready or buckets not created yet"