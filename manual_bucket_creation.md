# Quick Fix: Create MinIO Buckets Manually

If you don't see the bronze, silver, and gold buckets in MinIO, follow these steps:

## Option 1: Using the Script (Easiest)

```bash
# Make the script executable
chmod +x create_minio_buckets.sh

# Run it
./create_minio_buckets.sh
```

## Option 2: Manual Commands

Run these commands in your terminal:

```bash
# First, ensure MinIO is running
docker ps | grep minio

# Create buckets using docker exec
docker exec minio sh -c "
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc mb myminio/bronze
mc mb myminio/silver
mc mb myminio/gold
mc mb myminio/warehouse
mc ls myminio/
"
```

## Option 3: Using MinIO Web Console

1. Open your browser and go to: **http://localhost:9001**
2. Login with:
   - Username: `minioadmin`
   - Password: `minioadmin`
3. Click on **"Buckets"** in the left sidebar
4. Click **"Create Bucket"** button
5. Create these buckets one by one:
   - `bronze`
   - `silver`
   - `gold`
   - `warehouse`

## Option 4: Run the create-buckets service again

```bash
# From your project root
docker compose -f processing/docker-compose.yml up create-buckets
```

## Verify Buckets Were Created

After creating the buckets, verify they exist:

```bash
# Using make command
make check-buckets

# Or directly with docker
docker exec minio mc ls myminio/
```

You should see output like:
```
[2024-01-01 12:00:00 UTC]     0B bronze/
[2024-01-01 12:00:00 UTC]     0B silver/
[2024-01-01 12:00:00 UTC]     0B gold/
[2024-01-01 12:00:00 UTC]     0B warehouse/
```

## Common Issues

### "command not found: mc"
The MinIO client isn't installed in the container. Use this instead:
```bash
docker run --rm --network bakery-network minio/mc:latest \
  alias set myminio http://minio:9000 minioadmin minioadmin && \
  mc mb myminio/bronze myminio/silver myminio/gold myminio/warehouse && \
  mc ls myminio/
```

### "The specified bucket does not exist"
This means the buckets haven't been created yet. Follow the steps above to create them.

### Can't access MinIO Console
Make sure MinIO is running:
```bash
docker ps | grep minio
```

If not running, start it with:
```bash
make start
# or
docker compose -f processing/docker-compose.yml up -d minio
```