#VERSION=1.0
FROM seldonio/pyseldon:1.7

ENV SELDON_HOME /home/seldon
RUN mkdir -p  "$SELDON_HOME"
WORKDIR $SELDON_HOME

RUN git clone https://github.com/fergiemcdowall/reuters-21578-json

ADD build_model.py $SELDON_HOME/build_model.py

RUN python build_model.py

CMD [ "python","/home/seldon/scripts/start_recommendation_microservice.py","--recommender","/home/seldon/reuters_recommender"]



