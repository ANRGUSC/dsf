# Build from the repo root:
#   docker build -f examples/pipeline-ctg/tasks/sink/Dockerfile -t pipeline-ctg-sink:latest .
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pyzmq
COPY sdk/python/dsf_sdk ./dsf_sdk
COPY examples/pipeline-ctg/tasks/sink/task.py .
CMD ["python", "task.py"]
