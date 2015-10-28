FROM continuumio/anaconda

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN \
    apt-get update && \
    apt-get -y -q install build-essential git g++ gcc telnet procps 

RUN cd /usr/local/src && mkdir xgboost && cd xgboost && \
    git clone https://github.com/dmlc/xgboost.git && cd xgboost && \
    ./build.sh && \ 
    cd python-package  && python setup.py install && \
    cp -R /usr/local/src/xgboost/xgboost/wrapper/ /opt/conda/lib/python2.7/site-packages/xgboost-0.4-py2.7.egg/xgboost

RUN pip install kazoo filechunkio Flask gunicorn

ADD ./python-modules /python-modules/seldon

ENV PYTHONPATH $PYTHONPATH:/python-modules

ADD ./python-modules /python-modules/seldon

ADD ./scripts /scripts

# Define default command.
CMD ["bash"]



