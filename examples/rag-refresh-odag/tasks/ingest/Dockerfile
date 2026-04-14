# Build from the repo root:
#   docker build -f examples/rag-refresh-odag/tasks/ingest/Dockerfile \
#     -t <REGISTRY>/dsf-rag-ingest:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/dsf_sdk ./dsf_sdk
COPY examples/rag-refresh-odag/tasks/ingest/task.py .
CMD ["python", "task.py"]
