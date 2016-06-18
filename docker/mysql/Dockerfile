FROM mysql:5.6.29

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN mkdir -p /seldon-data/mysql
VOLUME /seldon-data

ADD my.cnf /etc/mysql/conf.d/seldon.cnf




