FROM seldonio/seldon-control:2.0.0_v1

RUN \
    apt-get update && \
    apt-get -y -q install unzip

ADD attr.json /attr.json

ADD create_ml10m_recommender.sh /create_ml10m_recommender.sh

