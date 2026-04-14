# Build from the repo root:
#   docker build -f examples/rag-refresh-odag/tasks/report/Dockerfile \
#     -t <REGISTRY>/dsf-rag-report:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/dsf_sdk ./dsf_sdk
COPY examples/rag-refresh-odag/tasks/report/task.py .
CMD ["python", "task.py"]
