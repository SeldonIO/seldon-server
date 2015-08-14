FROM continuumio/anaconda

ENV HOME /root
ENV DEBIAN_FRONTEND noninteractive

RUN \
    apt-get update && \
    apt-get -y -q install build-essential automake autoconf libxmu-dev gcc libpthread-stubs0-dev libtool libboost-program-options-dev libboost-python-dev zlib1g-dev libc6 libgcc1 libstdc++6 libblas-dev liblapack-dev git

# Install and make vw (Vowpal Wabbit) 

RUN cd /usr/local/src;git clone https://github.com/JohnLangford/vowpal_wabbit.git;cd vowpal_wabbit;git checkout 7.10;./autogen.sh;./configure;make;make test;make install

# perf

RUN cd /usr/local/src;wget http://osmot.cs.cornell.edu/kddcup/perf/perf.src.tar.gz;tar xvf perf.src.tar.gz;rm perf.src.tar.gz;mv perf.src perf;cd perf;make -B perf;make install

ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/lib

RUN cd /usr/local/src; git clone https://github.com/mokelly/wabbit_wappa.git ; cd wabbit_wappa ; python setup.py install

RUN pip install kazoo filechunkio

ADD ./python-modules /python-modules/seldon

ENV PYTHONPATH $PYTHONPATH:/python-modules

ADD ./scripts /scripts

# Define default command.
CMD ["bash"]
