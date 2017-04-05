FROM seldonio/pyseldon:%SELDON_PYTHON_PACKAGE_VERSION%

ENV SELDON_HOME=/home/seldon

#RUN cd $SELDON_HOME && mkdir data

COPY data/indicators_nan_replaced.csv $SELDON_HOME/data/indicators_nan_replaced.csv
#COPY data/indicators_nan_replacedv2.csv $SELDON_HOME/data/indicators_nan_replacedv2.csv
#COPY data/indicators_nan_replacedv3.csv $SELDON_HOME/data/indicators_nan_replacedv3.csv

COPY create_pipeline.py $SELDON_HOME/create_pipeline.py

COPY train_model.sh $SELDON_HOME/train_model.sh

COPY run_microservice.sh /run_microservice.sh

CMD ["/run_microservice.sh"]
