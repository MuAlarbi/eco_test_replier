# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir fastapi uvicorn nats-py httpx paho-mqtt requests eclipse-zenoh

# Make port 8001 available to the world outside this container
EXPOSE 8001

# Define environment variable
# ENV NAME World

# # Run app.py when the container launches
# CMD ["uvicorn", "replier:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
