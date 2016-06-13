FROM maven:3.3.3-jdk-8

ENV SELDON_HOME /home/seldon
RUN mkdir -p  "${SELDON_HOME}"
WORKDIR ${SELDON_HOME}

# create ${SELDON_HOME}/seldon-server-?.war
COPY seldon-server.tar.gz ${SELDON_HOME}/
RUN cd ${SELDON_HOME} && \
    tar xfz seldon-server.tar.gz && \
    cd seldon-server/stream && \
    mvn clean package && \
    mv -v ${SELDON_HOME}/seldon-server/stream/target/*.jar ${SELDON_HOME}/ && \
    rm -rf ${SELDON_HOME}/seldon-server && \
    rm -f ${SELDON_HOME}/seldon-server.tar.gz

# Define default command.
CMD ["bash"]

