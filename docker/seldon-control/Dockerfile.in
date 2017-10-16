FROM seldonio/pyseldon:%SELDON_PYTHON_PACKAGE_VERSION%

RUN \
    apt-get update && \
    apt-get -y -q install netcat dnsutils emacs vim mysql-client

RUN pip install memcache-cli awscli

RUN \
  apt-get update && \
  apt-get install -y openjdk-7-jdk && \
  rm -rf /var/lib/apt/lists/*

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

ENV hadoop_ver 2.6.5
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
#    curl http://www.apache.org/dist/spark/spark-${spark_ver}/spark-${spark_ver}-bin-hadoop2.6.tgz | \
RUN mkdir -p /opt && \
    cd /opt && \
    curl http://archive.apache.org/dist/spark/spark-${spark_ver}/spark-${spark_ver}-bin-hadoop2.6.tgz | \
        tar -zx && \
    ln -s spark-${spark_ver}-bin-hadoop2.6 spark && \
    echo Spark ${spark_ver} installed in /opt

ENV PATH $PATH:/opt/spark/bin

RUN pip install luigi
COPY luigi.cfg /home/seldon/luigi.cfg
ENV LUIGI_CONFIG_PATH /home/seldon/luigi.cfg

# add Kafkacat for debugging kafka
RUN mkdir -p /opt && \ 
    cd /opt && \
    git clone https://github.com/edenhill/librdkafka && \
    cd librdkafka && \ 
    ./configure && \
    make && make install
RUN mkdir -p /opt && \ 
    cd /opt && \
    git clone https://github.com/edenhill/kafkacat && \
    cd kafkacat && \ 
    ./configure && \
    make && make install

ENV SELDON_HOME /home/seldon
RUN mkdir -p  "$SELDON_HOME"
WORKDIR $SELDON_HOME

ARG SELDON_SPARK_VERSION
COPY ./seldon-spark-${SELDON_SPARK_VERSION}-jar-with-dependencies.jar $SELDON_HOME/libs/

RUN ln -s $SELDON_HOME/libs/seldon-spark-${SELDON_SPARK_VERSION}-jar-with-dependencies.jar $SELDON_HOME/libs/seldon-spark.jar

COPY seldon.conf /home/seldon
RUN (mkdir /root/.seldon && ln -s /home/seldon/seldon.conf /root/.seldon/seldon.conf)

RUN mkdir -p /seldon-data/conf
RUN mkdir -p /seldon-data/seldon-models
VOLUME /seldon-data

COPY client-dashboard.json $SELDON_HOME

#
# Add Maven
#

ARG MAVEN_VERSION=3.3.9
ARG USER_HOME_DIR="/home/seldon"

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL http://apache.osuosl.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz \
    | tar -xzC /usr/share/maven --strip-components=1 \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

#COPY settings-docker.xml /usr/share/maven/ref/

VOLUME "$USER_HOME_DIR/.m2"

ADD grpc-util ${SELDON_HOME}/grpc-util

