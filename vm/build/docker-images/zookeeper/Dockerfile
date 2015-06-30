FROM seldonio/java7jre:0.1

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN (   apt-get update && \
        apt-get install -y wget python-pip && \
        wget -q -O - http://apache.mirrors.pair.com/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz | tar -xzf - -C /opt && \
        mv /opt/zookeeper-3.4.6 /opt/zookeeper && \
        cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg && \
        mkdir -p /tmp/zookeeper && \
	pip install kazoo && \
        apt-get remove -y --auto-remove wget && \
        apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*)

EXPOSE 2181 2888 3888

WORKDIR /opt/zookeeper

VOLUME ["/opt/zookeeper/conf", "/tmp/zookeeper"]

ENTRYPOINT ["/opt/zookeeper/bin/zkServer.sh"]
CMD ["start-foreground"]

