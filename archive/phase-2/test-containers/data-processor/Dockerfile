FROM python:3.11-slim

RUN pip install pyzmq

WORKDIR /app

COPY process.py .

CMD ["python", "process.py"]
