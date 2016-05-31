FROM kubernetes/heapster_influxdb:v0.6

RUN mkdir -p /seldon-data/influxdb
VOLUME /seldon-data

ADD config.toml /etc/influxdb.toml



