# ---- Flink Stream Processor ----
# Official Flink image with Python support (PyFlink pre-installed)
FROM flink:1.18

# Install Python 3 and pip (not included in the base Flink image)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Download Kafka connector JAR (uber JAR — includes all dependencies)
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Install apache-flink Python package and dependencies
COPY requirements.txt /opt/flink/jobs/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/jobs/requirements.txt

# Copy processor source into the jobs directory
COPY . /opt/flink/jobs/processor/

# Ensure processor package is importable
ENV PYTHONPATH="/opt/flink/jobs:${PYTHONPATH}"
