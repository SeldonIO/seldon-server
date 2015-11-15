FROM continuumio/anaconda

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN \
    apt-get update && \
    apt-get -y -q install build-essential automake autoconf libxmu-dev g++ gcc libpthread-stubs0-dev libtool libboost-program-options-dev libboost-python-dev zlib1g-dev libc6 libgcc1 libstdc++6 libblas-dev liblapack-dev git telnet procps

RUN cd /usr/local/src && mkdir xgboost && cd xgboost && \
    git clone https://github.com/dmlc/xgboost.git && cd xgboost && \
    ./build.sh && \ 
    cd python-package  && python setup.py install && \
    cp -R /usr/local/src/xgboost/xgboost/wrapper/ /opt/conda/lib/python2.7/site-packages/xgboost-0.4-py2.7.egg/xgboost

# Install and make vw (Vowpal Wabbit) 

RUN cd /usr/local/src;git clone https://github.com/JohnLangford/vowpal_wabbit.git;cd vowpal_wabbit;git checkout 8.1 ;./autogen.sh;./configure;make;make test;make install


RUN cd /usr/local/src; git clone https://github.com/seldonio/wabbit_wappa.git  ; cd wabbit_wappa ; python setup.py install

# perf

RUN cd /usr/local/src;wget http://osmot.cs.cornell.edu/kddcup/perf/perf.src.tar.gz;tar xvf perf.src.tar.gz;rm perf.src.tar.gz;mv perf.src perf;cd perf;make -B perf;make install

RUN pip install kazoo filechunkio Flask gunicorn

# keras

RUN cd /usr/local/src && mkdir keras && cd keras && \
       git clone https://github.com/fchollet/keras.git && \
       cd keras && git checkout tags/0.2.0 && python setup.py install

# bayes_opt

RUN cd /ur/local/src; git clone https://github.com/fmfn/BayesianOptimization ; cd BayesianOptimization ; python setup.py install

ADD ./python-modules /python-modules/seldon

ENV PYTHONPATH $PYTHONPATH:/python-modules
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/lib

ADD ./python-modules /python-modules/seldon

ADD ./scripts /scripts

# Define default command.
CMD ["bash"]


