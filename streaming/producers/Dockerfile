FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer scripts
COPY pos_producer.py .
COPY iot_producer.py .
COPY config.py .

# Default command (overridden by docker-compose)
CMD ["python", "pos_producer.py"]