#VERSION=0.1
FROM seldonio/pyseldon

RUN \
    apt-get update && \
    apt-get -y -q install netcat dnsutils emacs vim mysql-client

RUN pip install memcache-cli

RUN mkdir -p $SELDON_HOME/bin
ENV PATH $SELDON_HOME/bin:$PATH

