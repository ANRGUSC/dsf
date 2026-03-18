FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    torch --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir transformers

RUN mkdir -p /app/gpt2_dag
COPY gpt2-dag/src/gpt2_dag/tensor_dag.py /app/gpt2_dag/tensor_dag.py
RUN echo 'from .tensor_dag import GPT2TensorDAGLMHeadModel, cache_to_tuples, tuples_to_cache' > /app/gpt2_dag/__init__.py
COPY dsf/phase-1/example/gpt2-layer-dag/layer_node.py /app/layer_node.py

ENV PYTHONPATH=/app
WORKDIR /app
