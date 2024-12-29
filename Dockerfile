# Use an official Python image as the base
FROM python:3.10-slim

# Install required tools and libraries
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    cmake \
    git \
    curl \
    libhiredis-dev \
    redis-server \
    && rm -rf /var/lib/apt/lists/*

# Download and install nlohmann/json (header-only library)
RUN mkdir -p /usr/local/include/nlohmann && \
    curl -L https://github.com/nlohmann/json/releases/latest/download/json.hpp -o /usr/local/include/nlohmann/json.hpp

# Clone the redis_task repository
RUN git clone https://github.com/kcherneha/redis_task.git /app/redis_task

# Set the working directory
WORKDIR /app/redis_task

# Build the project
RUN g++ -std=c++17 -o consumer_queue task_3.cpp task_3_main.cpp \
    -I/usr/include -I/usr/local/include \
    -L/usr/local/lib -lhiredis -lpthread

# Install Python dependencies
COPY requirements.txt /app/redis_task/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script
COPY redis_publisher.py /app/redis_task/redis_publisher.py

# Expose the Redis port
EXPOSE 8080

# Start Redis server, the C++ application, and the Python script with log redirection
CMD ["sh", "-c", "redis-server --daemonize yes && ./consumer_queue 4 127.0.0.1 6379 > /app/redis_task/consumer_queue.log 2>&1 & python redis_publisher.py > /app/redis_task/redis_publisher.log 2>&1"]
