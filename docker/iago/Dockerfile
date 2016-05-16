#VERSION=0.1
FROM maven:3.3.3-jdk-8

RUN \
    apt-get update && \
    apt-get -y -q install build-essential automake autoconf zlib1g-dev libc6 libgcc1 libstdc++6 git telnet procps

RUN git clone https://github.com/twitter/iago && \
    cd iago && \
    mvn package -DskipTests

RUN cd iago && \
    mkdir tmp; cd tmp && \
    unzip ../target/iago-*-package-dist.zip

#remove this when done - or add to above apt-get
RUN apt-get -y -q install emacs

COPY web.ramp-up.scala.in /iago/tmp
COPY replay.txt /iago/tmp
COPY start-load-test.sh /


