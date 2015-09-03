FROM seldonio/xgboost_train

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN \
    apt-get update && \
    apt-get -y -q install telnet procps

RUN pip install Flask gunicorn

ADD ./scripts /xgboost/xgboost_runtime

# Define default command.
CMD ["bash"]
