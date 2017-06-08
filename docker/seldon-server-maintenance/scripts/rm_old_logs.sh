#!/bin/sh

gzip  /home/seldon/logs/base/api.log.`date  --date='1 days ago' --iso-8601`
rm  /home/seldon/logs/base/api.log.`date  --date='2 days ago' --iso-8601`.gz

gzip  /home/seldon/logs/base/restapi.log.`date  --date='1 days ago' --iso-8601`
rm  /home/seldon/logs/base/restapi.log.`date  --date='2 days ago' --iso-8601`.gz

rm  /home/seldon/logs/base/ctr.log.`date  --date='3 days ago' --iso-8601`
rm  /home/seldon/logs/base/ctr-alg.log.`date  --date='3 days ago' --iso-8601`

gzip  /home/seldon/logs/tomcat/localhost_access_log.`date  --date='1 days ago' --iso-8601`.txt
rm  /home/seldon/logs/tomcat/localhost_access_log.`date  --date='2 days ago' --iso-8601`.txt.gz

rm  /home/seldon/logs/actions/actions.log.`date  --date='2 days ago' --iso-8601`
