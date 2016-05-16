FROM continuumio/anaconda:2.4.1

RUN \
    apt-get update && \
    apt-get -y -q install build-essential memcached libmemcached-dev mysql-client-5.5 mysql-common libmysqlclient-dev

RUN pip install python-daemon
RUN pip install kazoo filechunkio Flask gunicorn pylibmc gensim annoy smart_open
RUN pip install cmd2 MySQL-python

