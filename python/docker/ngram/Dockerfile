FROM seldonio/pyseldon

# install kenlm
RUN apt-get -y -q  install build-essential libboost-all-dev zlib1g-dev libbz2-dev liblzma-dev

RUN wget -O - http://kheafield.com/code/kenlm.tar.gz |tar xz ; cd  kenlm ; ./bjam -j4 

ADD ./ngram_scripts /ngram_scripts

