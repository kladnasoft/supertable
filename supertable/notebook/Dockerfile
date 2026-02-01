# Use a slim python image
FROM python:3.10-slim

# Install common notebook libraries
RUN pip install --no-cache-dir requests

# Create a non-privileged user
RUN useradd -m sandbox
USER sandbox
WORKDIR /home/sandbox

# Set a timeout/memory limit via environment variables if needed
ENV PYTHONUNBUFFERED=1