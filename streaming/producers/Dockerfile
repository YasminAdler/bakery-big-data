FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer scripts
COPY . .

# Default command - can be overridden
CMD ["python", "-u", "generate_sales_events.py"] 