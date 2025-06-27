#!/usr/bin/env python3
"""
Create MinIO buckets for the bakery data lake
"""
import boto3
from botocore.exceptions import ClientError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_minio_buckets():
    """Create bronze, silver, and gold buckets in MinIO"""
    
    # MinIO connection settings
    minio_endpoint = "http://minio:9000"  # Use 'minio' when running inside Docker
    # minio_endpoint = "http://localhost:9000"  # Use 'localhost' when running outside Docker
    access_key = "minioadmin"
    secret_key = "minioadmin"
    
    # Create S3 client for MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=False,
        verify=False
    )
    
    # List of buckets to create
    buckets = ['bronze', 'silver', 'gold', 'warehouse']
    
    for bucket_name in buckets:
        try:
            # Check if bucket exists
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"✅ Bucket '{bucket_name}' already exists")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    s3_client.create_bucket(Bucket=bucket_name)
                    logger.info(f"✅ Created bucket '{bucket_name}'")
                except Exception as create_error:
                    logger.error(f"❌ Failed to create bucket '{bucket_name}': {create_error}")
            else:
                logger.error(f"❌ Error checking bucket '{bucket_name}': {e}")
    
    # List all buckets to confirm
    try:
        response = s3_client.list_buckets()
        logger.info("\n📦 All buckets in MinIO:")
        for bucket in response['Buckets']:
            logger.info(f"  - {bucket['Name']}")
    except Exception as e:
        logger.error(f"❌ Failed to list buckets: {e}")

if __name__ == "__main__":
    create_minio_buckets()