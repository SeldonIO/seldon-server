FROM mysql:5.6.21

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive
RUN mkdir -p /etc/mysql
ADD my.cnf /etc/mysql/my.cnf

RUN usermod -u 1000 mysql

