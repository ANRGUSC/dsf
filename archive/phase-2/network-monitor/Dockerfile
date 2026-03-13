FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    clang \
    llvm \
    linux-headers-generic \
    libbpf-dev \
    libelf-dev \
    zlib1g-dev \
    make \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy source files
COPY network_monitor.bpf.c network_monitor.h monitor.c Makefile vmlinux.h ./

# Build the eBPF program and user-space loader
RUN make

# Create directory for metrics output
RUN mkdir -p /var/run

# Set entrypoint
ENTRYPOINT ["./monitor"]

# Default arguments: update interval (seconds), metrics file path
CMD ["5", "/var/run/network_metrics.prom"]
