FROM continuumio/anaconda

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN \
    apt-get update && \
    apt-get -y -q install build-essential automake autoconf libxmu-dev gcc libpthread-stubs0-dev libtool libboost-program-options-dev libboost-python-dev zlib1g-dev libc6 libgcc1 libstdc++6 libblas-dev liblapack-dev git && \
    pip install setuptools


RUN cd /usr/local/src && mkdir xgboost && cd xgboost && \
    git clone https://github.com/dmlc/xgboost.git && cd xgboost && \
    ./build.sh && \ 
    cd python-package && python setup.py install

ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/lib

RUN pip install kazoo filechunkio

#ADD ./python-modules /python-modules/seldon

ENV PYTHONPATH $PYTHONPATH:/python-modules

#ADD ./scripts /xgboost/xgboost_train

# Define default command.
CMD ["bash"]
