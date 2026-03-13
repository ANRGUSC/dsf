FROM ubuntu:latest
RUN apt-get update && apt-get install -y procps && apt-get install -y build-essential tar gzip
COPY pathload.tar.gz /opt/
WORKDIR /opt
RUN gunzip pathload.tar.gz \
    && tar xvf pathload.tar \
    && cd pathload_1.3.2 \
    && ./configure \
    && make
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
WORKDIR /opt/pathload_1.3.2/
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

