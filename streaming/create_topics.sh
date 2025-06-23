#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 10

# Function to create topic
create_topic() {
    topic=$1
    echo "Creating topic: $topic"
    docker exec kafka kafka-topics --create \
        --topic $topic \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists 2>/dev/null || echo "Topic $topic already exists"
}

# Create all topics
create_topic "bakery-pos-events"
create_topic "bakery-iot-events"
create_topic "bakery-inventory-updates"
create_topic "bakery-customer-feedback"
create_topic "bakery-promotions"
create_topic "bakery-weather-data"

echo -e "\nAll topics created! Listing topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092