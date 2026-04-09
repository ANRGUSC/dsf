# Build from the repo root:
#   docker build -f examples/sensor-fusion-ctg/Dockerfile -t 192.168.1.163:5000/sensor-fusion-ctg:latest .
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pyzmq
COPY sdk/python/dsf_sdk ./dsf_sdk
COPY examples/sensor-fusion-ctg/task.py .
CMD ["python", "task.py"]
