FROM ubuntu:trusty

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN ( apt-get update && \
    apt-get install -y python-pip openjdk-7-jre-headless )

ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

RUN pip install awscli kazoo boto FileChunkIO

ADD ./scripts /semvec

# Define default command.
CMD ["bash"]




