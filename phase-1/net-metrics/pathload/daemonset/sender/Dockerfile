FROM ubuntu:22.04

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy pathload source code
COPY pathload /tmp/pathload

# Copy multithreaded wrapper
COPY multithreaded-sender-wrapper.c /tmp/

# Build pathload
WORKDIR /tmp/pathload
RUN ./configure && \
    make && \
    cp pathload_snd /usr/local/bin/ && \
    cp pathload_rcv /usr/local/bin/

# Build multithreaded wrapper
WORKDIR /tmp
RUN gcc -o multithreaded-sender multithreaded-sender-wrapper.c -lpthread && \
    cp multithreaded-sender /usr/local/bin/

# Install k3s kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && \
    mv kubectl /usr/local/bin/

ENTRYPOINT ["multithreaded-sender"]
