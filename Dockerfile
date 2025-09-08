# Use official Python runtime as a base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Set the working directory in the container
WORKDIR /app

# Install system dependencies required for building Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .


# Health check to ensure the container is running properly
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import redis; r = redis.Redis(host='${REDIS_HOST}', port=${REDIS_PORT}, db=${REDIS_DB}, password='${REDIS_PASSWORD}'); r.ping()" || exit 1

# Command to run the application
CMD ["python", "main.py"]
