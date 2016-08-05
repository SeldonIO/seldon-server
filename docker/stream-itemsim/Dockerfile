#FROM continuumio/anaconda:2.4.1
FROM ubuntu:14.04

RUN \
    apt-get update && \
    apt-get -y -q install netcat dnsutils emacs vim mysql-client libmysqlclient-dev mysql-common software-properties-common python python-dev python-pip

RUN \
  add-apt-repository ppa:openjdk-r/ppa && \
  apt-get update && \
  apt-get install -y openjdk-8-jdk && \
  rm -rf /var/lib/apt/lists/*

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN pip install kafka-python MySQL-python

ENV SELDON_HOME /home/seldon
RUN mkdir -p  "$SELDON_HOME"
WORKDIR $SELDON_HOME

ARG SELDON_STREAM_VERSION
COPY ./seldon-stream-${SELDON_STREAM_VERSION}-jar-with-dependencies.jar $SELDON_HOME/libs/

RUN ln -s $SELDON_HOME/libs/seldon-stream-${SELDON_STREAM_VERSION}-jar-with-dependencies.jar $SELDON_HOME/libs/seldon-stream.jar

COPY scripts/itemsim-kafka-to-mysql.py $SELDON_HOME/python/

