# Use the official Python 3.11.0 slim image as the base
FROM python:3.11-slim

# Set environment variables to prevent Python from writing pyc files and buffer outputs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies: ghostscript and git
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ghostscript \
        git \
        openssh-client \
        curl \
        gcc \
        gh \
        build-essential \
        libglib2.0-0 \
        libgl1-mesa-glx \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip globally as root
RUN pip install --upgrade pip setuptools wheel


# Set the working directory
WORKDIR /workspace

# Copy the rest of the application code
COPY . .

# Install Ruff and Black for linting and formatting
RUN pip install ruff black