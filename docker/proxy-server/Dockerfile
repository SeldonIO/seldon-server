FROM nginx:alpine

ENV BASIC_AUTH_FILE /etc/nginx/basic_auth
ENV NGINX_PORT 80

ADD proxy.conf /etc/nginx/conf.d/default.conf
ADD entrypoint.sh /entrypoint.sh
CMD ["/entrypoint.sh"]