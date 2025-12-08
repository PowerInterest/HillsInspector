FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm

# Install system dependencies for pdf2image and opencv/easyocr
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    poppler-utils \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Install ruff and ty as tools available globally
RUN uv tool install ruff
RUN uv tool install ty

# Set environment variable for EasyOCR to default to CPU
ENV EASYOCR_GPU=False
