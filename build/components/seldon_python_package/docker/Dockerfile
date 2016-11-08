FROM continuumio/anaconda:4.1.1

RUN \
    apt-get update && \
    apt-get -y -q install build-essential memcached libmemcached-dev mysql-client-5.5 mysql-common libmysqlclient-dev unzip

RUN pip install python-daemon
RUN pip install kazoo filechunkio Flask gunicorn pylibmc gensim annoy smart_open
RUN pip install cmd2 MySQL-python 
# install proto buf
RUN mkdir -p /tmp/protoc && \
    curl -L https://github.com/google/protobuf/releases/download/v3.0.0/protoc-3.0.0-linux-x86_64.zip > /tmp/protoc/protoc.zip && \
    cd /tmp/protoc && \
    unzip protoc.zip && \
    cp /tmp/protoc/bin/protoc /usr/local/bin && \
    chmod go+rx /usr/local/bin/protoc && \
    cd /tmp && \
    rm -r /tmp/protoc

ENV GRPC_PYTHON_VERSION 1.0.0
RUN pip install grpcio==${GRPC_PYTHON_VERSION} grpcio-tools==${GRPC_PYTHON_VERSION}

