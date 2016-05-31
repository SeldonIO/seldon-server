FROM seldonio/td-agent:1.0

EXPOSE 24224

ADD td-agent.conf /etc/td-agent/td-agent.conf

RUN mkdir -p /seldon-data/logs/events
RUN mkdir -p /seldon-data/logs/actions
RUN mkdir -p /seldon-data/logs/stats
VOLUME /seldon-data

