FROM python:3.9-slim
WORKDIR /app

COPY ../requirements.txt .

COPY requirements.txt* ./

RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer scripts
COPY . .

# The command will be overridden by docker-compose
CMD ["python", "pos_producer.py"]