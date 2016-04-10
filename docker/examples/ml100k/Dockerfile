#VERSION=1.0
FROM seldonio/seldon-control:1.2.3_v9

RUN \
    apt-get update && \
    apt-get -y -q install unzip

ADD attr.json /attr.json

ADD create_ml100k_recommender.sh /create_ml100k_recommender.sh







