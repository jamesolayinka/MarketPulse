# Use the official Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy code into the container
COPY ./src /app/src

# Default command (can be overridden)
CMD ["python", "src/kafka_producer.py"]
