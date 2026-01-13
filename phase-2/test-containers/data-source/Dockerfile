FROM python:3.11-slim

RUN pip install pyzmq

WORKDIR /app

COPY producer.py .

CMD ["python", "producer.py"]
