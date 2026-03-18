FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    torch --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir transformers pyzmq

RUN mkdir -p /app/gpt2_dag
COPY gpt2-dag/src/gpt2_dag/tensor_dag.py /app/gpt2_dag/tensor_dag.py
RUN echo 'from .tensor_dag import GPT2TensorDAGLMHeadModel, cache_to_tuples, tuples_to_cache' > /app/gpt2_dag/__init__.py

COPY dsf/phase-2/example/gpt2-layer-ctg/layer_node_ctg.py /app/layer_node_ctg.py
COPY dsf/phase-2/example/gpt2-layer-ctg/feeder.py /app/feeder.py

RUN python -c "from transformers import GPT2LMHeadModel, GPT2Tokenizer; \
    GPT2LMHeadModel.from_pretrained('gpt2', cache_dir='/app/model_cache'); \
    GPT2Tokenizer.from_pretrained('gpt2', cache_dir='/app/model_cache')"

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV HF_HUB_OFFLINE=1
WORKDIR /app
