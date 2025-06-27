#!/bin/bash

echo "🔧 MinIO Bucket Creation Script"
echo "================================"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if MinIO is running
echo -e "\n${YELLOW}Step 1: Checking if MinIO is running...${NC}"
if docker ps | grep -q "minio"; then
    echo -e "${GREEN}✓ MinIO container is running${NC}"
else
    echo -e "${RED}✗ MinIO container is not running${NC}"
    echo "Please start MinIO first with: make start"
    exit 1
fi

# Method 1: Run the create-buckets service
echo -e "\n${YELLOW}Step 2: Running create-buckets service...${NC}"
cd /workspace
docker compose -f processing/docker-compose.yml up create-buckets

# Method 2: Manual creation using docker exec
echo -e "\n${YELLOW}Step 3: Manually creating buckets (backup method)...${NC}"
docker exec minio sh -c "
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc mb myminio/bronze --ignore-existing
mc mb myminio/silver --ignore-existing
mc mb myminio/gold --ignore-existing
mc mb myminio/warehouse --ignore-existing
echo '✓ All buckets created'
mc ls myminio/
"

# Method 3: Using MinIO client from host (if installed)
echo -e "\n${YELLOW}Step 4: Verifying buckets...${NC}"
echo "Checking buckets via docker exec:"
docker exec minio mc ls myminio/ 2>/dev/null || docker exec minio sh -c "mc alias set myminio http://localhost:9000 minioadmin minioadmin && mc ls myminio/"

echo -e "\n${GREEN}✓ Bucket creation complete!${NC}"
echo -e "\nYou can verify the buckets by:"
echo "1. Visiting MinIO Console at http://localhost:9001"
echo "   Username: minioadmin"
echo "   Password: minioadmin"
echo "2. Running: make check-buckets"
echo "3. Running: docker exec minio mc ls myminio/"