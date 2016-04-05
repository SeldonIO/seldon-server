FROM seldonio/pyseldon:1.10

RUN \
    apt-get update && \
    apt-get -y -q install netcat dnsutils emacs vim mysql-client

RUN pip install memcache-cli awscli

ENV SELDON_HOME /home/seldon
RUN mkdir -p  "$SELDON_HOME"
WORKDIR $SELDON_HOME

COPY ./*.jar $SELDON_HOME/libs/

ENV SELDON_TAG v1.2.3
RUN git clone https://github.com/SeldonIO/seldon-server --branch ${SELDON_TAG}

RUN \
  apt-get update && \
  apt-get install -y openjdk-7-jdk && \
  rm -rf /var/lib/apt/lists/*

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

ENV hadoop_ver 2.6.1
ENV spark_ver 1.5.2

# Get Hadoop from US Apache mirror and extract just the native
# libs. (Until we care about running HDFS with these containers, this
# is all we need.)
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://www.apache.org/dist/hadoop/common/hadoop-${hadoop_ver}/hadoop-${hadoop_ver}.tar.gz | \
        tar -zx hadoop-${hadoop_ver}/lib/native && \
    ln -s hadoop-${hadoop_ver} hadoop && \
    echo Hadoop ${hadoop_ver} native libraries installed in /opt/hadoop/lib/native

# Get Spark from US Apache mirror.
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://www.apache.org/dist/spark/spark-${spark_ver}/spark-${spark_ver}-bin-hadoop2.6.tgz | \
        tar -zx && \
    ln -s spark-${spark_ver}-bin-hadoop2.6 spark && \
    echo Spark ${spark_ver} installed in /opt

ENV PATH $PATH:/opt/spark/bin

COPY seldon.conf /home/seldon
RUN (mkdir /root/.seldon && ln -s /home/seldon/seldon.conf /root/.seldon/seldon.conf)

RUN mkdir -p /seldon-data/conf
RUN mkdir -p /seldon-data/seldon-models
VOLUME /seldon-data


