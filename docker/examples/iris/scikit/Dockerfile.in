FROM seldonio/pyseldon:%SELDON_PYTHON_PACKAGE_VERSION%

ENV SELDON_HOME=/home/seldon
COPY scikit_pipeline.py $SELDON_HOME/scikit_pipeline.py
COPY create-json.py $SELDON_HOME/create-json.py

RUN  mkdir -p $SELDON_HOME/data && cd $SELDON_HOME/data ; wget --quiet http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data

RUN mkdir -p $SELDON_HOME/data/iris/events/1 && cat $SELDON_HOME/data/iris.data | python $SELDON_HOME/create-json.py | shuf > $SELDON_HOME/data/iris/events/1/iris.json

RUN cd $SELDON_HOME && python $SELDON_HOME/scikit_pipeline.py --events data/iris/events/1 --models /data/iris/scikit_models/1

COPY run_microservice.sh /run_microservice.sh

CMD ["/run_microservice.sh"]

