FROM grafana/grafana:2.6.0

RUN apt-get update && \
    apt-get install -y curl

RUN mkdir -p /seldon-data/grafana
VOLUME /seldon-data

COPY run.sh /run.sh

EXPOSE 3000
ENTRYPOINT /run.sh

