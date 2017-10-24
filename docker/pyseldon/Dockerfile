FROM continuumio/anaconda:4.1.1

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN conda install  -f  numpy

RUN \
    apt-get update && \
    apt-get -y -q install build-essential automake autoconf libxmu-dev g++ gcc libpthread-stubs0-dev libtool libboost-program-options-dev libboost-python-dev zlib1g-dev libc6 libgcc1 libstdc++6 libblas-dev liblapack-dev git telnet procps memcached libmemcached-dev mysql-client-5.5 mysql-common libmysqlclient-dev unzip

# opne issues mean we can't use latest xgboost with the sparse matrices we sometime create in python pipelines
#https://github.com/dmlc/xgboost/issues/1238
#https://github.com/dmlc/xgboost/issues/1583
RUN pip install xgboost==0.4a30
RUN cd /tmp/ && \
    TF_WHL_FILE=tensorflow-0.10.0rc0-cp27-none-linux_x86_64.whl && \
    wget https://storage.googleapis.com/tensorflow/linux/cpu/${TF_WHL_FILE} && \
    pip install ${TF_WHL_FILE} && \
    rm ${TF_WHL_FILE}

#RUN cd /usr/local/src && mkdir xgboost && cd xgboost && \
#    git clone https://github.com/dmlc/xgboost.git && cd xgboost && \
#    git checkout tags/v0.40 && \
#    ./build.sh && \
#    cd python-package  && python setup.py install && \
#    cp -R /usr/local/src/xgboost/xgboost/wrapper/ /opt/conda/lib/python2.7/site-packages/xgboost-0.4-py2.7.egg/xgboost

# Install and make vw (Vowpal Wabbit)

RUN cd /usr/local/src;git clone https://github.com/JohnLangford/vowpal_wabbit.git;cd vowpal_wabbit;git checkout 8.1 ;./autogen.sh;./configure;make;make test;make install


RUN cd /usr/local/src; git clone https://github.com/seldonio/wabbit_wappa.git  ; cd wabbit_wappa ; python setup.py install

# perf

RUN cd /usr/local/src;wget http://osmot.cs.cornell.edu/kddcup/perf/perf.src.tar.gz;tar xvf perf.src.tar.gz;rm perf.src.tar.gz;mv perf.src perf;cd perf;make -B perf;make install

RUN pip install kazoo filechunkio Flask gunicorn pylibmc gensim annoy smart_open

# keras

RUN cd /usr/local/src && mkdir keras && cd keras && \
       git clone https://github.com/fchollet/keras.git && \
       cd keras && git checkout tags/0.2.0 && python setup.py install

# bayes_opt

RUN cd /ur/local/src; git clone https://github.com/fmfn/BayesianOptimization ; cd BayesianOptimization ; python setup.py install

# ensure latest sklearn

RUN pip install sklearn --upgrade

# ensure latest pandas

RUN pip install pandas --upgrade

# seldon shell requisites

RUN pip install cmd2 MySQL-python

# for ngram prediction
RUN pip install DAWG

RUN pip install -e git+git://github.com/SeldonIO/wabbit_wappa#egg=wabbit-wappa-3.0.2

RUN pip install -e git+git://github.com/fmfn/BayesianOptimization#egg=bayes_opt

###RUN pip install seldon==1.5.1
ARG SELDON_PYTHON_PACKAGE_VERSION
RUN echo "using SELDON_PYTHON_PACKAGE_VERSION[${SELDON_PYTHON_PACKAGE_VERSION}]"
COPY seldon-${SELDON_PYTHON_PACKAGE_VERSION}.tar.gz /tmp/
RUN \
    mkdir -p /tmp/install && \
    cd /tmp/install && \
    mv /tmp/seldon-${SELDON_PYTHON_PACKAGE_VERSION}.tar.gz . && \
    tar xfz seldon-${SELDON_PYTHON_PACKAGE_VERSION}.tar.gz && \
    cd seldon-${SELDON_PYTHON_PACKAGE_VERSION} && \
    pip install python-daemon && \
    python setup.py install && \
    rm -rf /tmp/install


ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/lib

ENV SELDON_HOME /home/seldon
ADD ./scripts $SELDON_HOME/scripts

# install proto buf
RUN mkdir -p /tmp/protoc && \
    curl -L https://github.com/google/protobuf/releases/download/v3.0.0/protoc-3.0.0-linux-x86_64.zip > /tmp/protoc/protoc.zip && \
    cd /tmp/protoc && \
    unzip protoc.zip && \
    cp /tmp/protoc/bin/protoc /usr/local/bin && \
    chmod go+rx /usr/local/bin/protoc && \
    cd /tmp && \
    rm -r /tmp/protoc

ENV GRPC_PYTHON_VERSION 1.0.0
RUN pip install grpcio==${GRPC_PYTHON_VERSION} grpcio-tools==${GRPC_PYTHON_VERSION}

# Define default command.
CMD ["bash"]

