#!/bin/sh

TARGET="${TARGET-}"
if [ -z "${TARGET}" ]; then
    echo "TARGET is required"
    exit 1
fi

BASIC_AUTH_FILE="${BASIC_AUTH_FILE-}"
if [ -z "${BASIC_AUTH_FILE}" ]; then
    echo "BASIC_AUTH_FILE is required"
    exit 1
fi

sed -i.bak s:TARGET:${TARGET}:g /etc/nginx/conf.d/default.conf
sed -i.bak s:NGINX_PORT:${NGINX_PORT}:g /etc/nginx/conf.d/default.conf
sed -i.bak s:BASIC_AUTH_FILE:${BASIC_AUTH_FILE}:g /etc/nginx/conf.d/default.conf

exec nginx -g "daemon off;"
