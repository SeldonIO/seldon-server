FROM seldonio/pyseldon:%SELDON_PYTHON_PACKAGE_VERSION%

ENV SELDON_HOME=/home/seldon

COPY inne_pipeline.py $SELDON_HOME/inne_pipeline.py
COPY create-json.py $SELDON_HOME/create-json.py
COPY DataGenerator.py $SELDON_HOME/DataGenerator.py

RUN  mkdir -p $SELDON_HOME/data && cd $SELDON_HOME/ && python DataGenerator.py --std 0.1 --nb-samples 1000 --dim 4 --nb-clusters 3 --fout data/simulation.csv

RUN mkdir -p $SELDON_HOME/data/simulation/events/1 && cat $SELDON_HOME/data/simulation.csv | python $SELDON_HOME/create-json.py | shuf > $SELDON_HOME/data/simulation/events/1/simulation.json

RUN cd $SELDON_HOME && python $SELDON_HOME/inne_pipeline.py --events data/simulation/events/1 --models /data/simulation/inne_models/1

COPY run_microservice.sh /run_microservice.sh

CMD ["/run_microservice.sh"]

