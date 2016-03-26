SHELL=/bin/bash

BASE_PATH=/data
MYSQL_DATA_PATH=${BASE_PATH}/mysql
CONF_DATA_PATH=${BASE_PATH}/conf
TDAGENT_DATA_PATH=${BASE_PATH}/seldon-data

mysql.yml: mysql.yml.in
	cat mysql.yml.in | sed 's=$$(DATA_PATH)=${MYSQL_DATA_PATH}=g' > mysql.yml

control.yml: control.yml.in
	cat control.yml.in | sed 's=$$(DATA_PATH)=${CONF_DATA_PATH}=g' > control.yml

td-agent-server.yml: td-agent-server.yml.in
	cat td-agent-server.yml.in | sed 's=$$(DATA_PATH)=${TDAGENT_DATA_PATH}=g' > td-agent-server.yml

clean:
	rm -f mysql.yml
	rm -f control.yml
	rm -f td-agent-server.yml
