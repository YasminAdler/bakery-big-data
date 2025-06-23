FROM python:3.9-slim

WORKDIR /app

# Install system dependencies (needed for some Python packages)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first (for better Docker caching)
COPY requirements.txt* ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt || \
    pip install --no-cache-dir \
    kafka-python==2.0.2 \
    faker==19.12.0 \
    numpy==1.24.3 \
    requests==2.31.0

# Copy all producer scripts
COPY . .

# Make Python output unbuffered (important for container logs)
ENV PYTHONUNBUFFERED=1

# The command will be overridden by docker-compose
CMD ["python", "pos_producer.py"]