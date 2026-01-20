FROM python:3.10-slim

# install measurement tools
RUN apt-get update && apt-get install -y \
    iperf3 iputils-ping && rm -rf /var/lib/apt/lists/*

# install k8s client
RUN pip install kubernetes

# copy script
COPY network-metrics.py /app/network-metrics.py
RUN chmod +x /app/network-metrics.py

# use python to run the script
ENTRYPOINT ["python3", "/app/network-metrics.py"]
