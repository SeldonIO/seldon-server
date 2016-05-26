FROM seldonio/seldon-control:%SELDON_CONTROL_IMAGE_VERSION%

ENV REUTERS_HOME /home/seldon/reuters
RUN mkdir -p  "$REUTERS_HOME"
WORKDIR $REUTERS_HOME

RUN git clone https://github.com/fergiemcdowall/reuters-21578-json

ADD create_csv.py $REUTERS_HOME/create_csv.py

ADD attr.json $REUTERS_HOME/attr.json

RUN python create_csv.py

ADD run_seldon_cli.sh /run_seldon_cli.sh

CMD ["/run_seldon_cli.sh"]

