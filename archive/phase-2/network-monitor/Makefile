# Makefile for building eBPF network monitor

CLANG ?= clang
LLC ?= llc
ARCH := $(shell uname -m | sed 's/x86_64/x86/' | sed 's/aarch64/arm64/')

# Kernel headers
KERNEL_HEADERS := /usr/src/linux-headers-$(shell uname -r)
LIBBPF_DIR := /usr/include

# BPF CFLAGS
BPF_CFLAGS := -g -O2 -target bpf -D__TARGET_ARCH_$(ARCH)
BPF_CFLAGS += -I. -I$(LIBBPF_DIR)

# User-space CFLAGS
USER_CFLAGS := -g -O2 -Wall
USER_LDFLAGS := -lbpf -lelf -lz

.PHONY: all clean

all: network_monitor.bpf.o monitor

# Compile eBPF program
network_monitor.bpf.o: network_monitor.bpf.c network_monitor.h
	$(CLANG) $(BPF_CFLAGS) -c network_monitor.bpf.c -o network_monitor.bpf.o

# Compile user-space program
monitor: monitor.c network_monitor.h
	$(CC) $(USER_CFLAGS) monitor.c -o monitor $(USER_LDFLAGS)

clean:
	rm -f network_monitor.bpf.o monitor

install: all
	install -m 0755 monitor /usr/local/bin/
	install -m 0644 network_monitor.bpf.o /usr/local/lib/
