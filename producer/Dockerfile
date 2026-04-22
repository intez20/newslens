# ---- Producer Service ----
# Lightweight Python image matching project target version
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first (layer caching — deps change less often than code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer source code into /app/producer/ so `python -m producer` finds the package
COPY . ./producer/

# Run the producer scheduler: polls sources → publishes to Kafka
CMD ["python", "-m", "producer"]
