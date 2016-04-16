#VERSION=1.1
FROM seldonio/seldon-control:1.3_v4

RUN \
    apt-get update && \
    apt-get -y -q install unzip

ADD attr.json /attr.json

ADD create_ml100k_recommender.sh /create_ml100k_recommender.sh







