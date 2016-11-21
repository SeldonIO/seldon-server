FROM seldonio/pyseldon:%SELDON_PYTHON_PACKAGE_VERSION%

ENV SELDON_HOME=/home/seldon
COPY create_pipeline.py $SELDON_HOME/create_pipeline.py

COPY train_model.sh $SELDON_HOME/train_model.sh
COPY load_model.sh $SELDON_HOME/load_model.sh
COPY run_microservice.sh /run_microservice.sh

CMD ["/run_microservice.sh"]
