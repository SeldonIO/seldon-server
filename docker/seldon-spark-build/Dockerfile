FROM maven:3.3.3-jdk-7

ENV SELDON_HOME /home/seldon
RUN mkdir -p  "$SELDON_HOME"
WORKDIR $SELDON_HOME

COPY seldon-server.tar.gz ${SELDON_HOME}/
RUN cd ${SELDON_HOME} && \
    tar xfz seldon-server.tar.gz && \
    rm -fv seldon-server.tar.gz && \
    cd seldon-server/offline-jobs/spark && \
    mvn clean package && \
    cd ${SELDON_HOME} && \
    cp seldon-server/offline-jobs/spark/target/*.jar . && \
    rm -rf seldon-server

# Define default command.
CMD ["bash"]



