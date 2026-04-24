# ---- Flink Stream Processor ----
# Official Flink image with Python support (PyFlink pre-installed)
FROM flink:1.18

# Install Python 3 and pip (not included in the base Flink image)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install apache-flink Python package and dependencies
COPY requirements.txt /opt/flink/jobs/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/jobs/requirements.txt

# Copy processor source into the jobs directory
COPY . /opt/flink/jobs/processor/
