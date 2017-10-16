FROM maven:3.3.3-jdk-8

ENV SELDON_HOME /home/seldon
RUN mkdir -p  "${SELDON_HOME}"
WORKDIR ${SELDON_HOME}

# create ${SELDON_HOME}/seldon-server-?.war
COPY seldon-server.tar.gz ${SELDON_HOME}/
RUN cd ${SELDON_HOME} && \
    tar xfz seldon-server.tar.gz && \
    cd seldon-server/server && \
    mvn clean package && \
    mv -v ${SELDON_HOME}/seldon-server/server/target/*.war ${SELDON_HOME}/ && \
    rm -rf ${SELDON_HOME}/seldon-server && \
    rm -f ${SELDON_HOME}/seldon-server.tar.gz

ENV CATALINA_HOME /usr/local/tomcat
ENV PATH $CATALINA_HOME/bin:$PATH
RUN mkdir -p "$CATALINA_HOME"
WORKDIR $CATALINA_HOME

ENV TOMCAT_MAJOR 7
ENV TOMCAT_VERSION 7.0.82
ENV TOMCAT_TGZ_URL https://www.apache.org/dist/tomcat/tomcat-$TOMCAT_MAJOR/v$TOMCAT_VERSION/bin/apache-tomcat-$TOMCAT_VERSION.tar.gz

RUN set -x \
	&& curl -fSL "$TOMCAT_TGZ_URL" -o tomcat.tar.gz \
	&& tar -xvf tomcat.tar.gz --strip-components=1 \
	&& rm bin/*.bat \
	&& rm tomcat.tar.gz*

EXPOSE 8080

RUN apt-get update && \
    apt-get install -y less telnet emacs

RUN (mkdir -p $SELDON_HOME/ROOT && \
    mv -v $SELDON_HOME/*.war $SELDON_HOME/ROOT && \
    cd $SELDON_HOME/ROOT && unzip *.war && rm *.war && \
    rm -rf $CATALINA_HOME/webapps/ROOT && ln -s $SELDON_HOME/ROOT $CATALINA_HOME/webapps/ROOT && \
    mkdir -p $SELDON_HOME/logs/base && \
    mkdir -p $SELDON_HOME/logs/actions && \
    mkdir -p $SELDON_HOME/logs/tomcat && \
    mkdir -p $CATALINA_HOME/logs/seldon-server && \
    ln -s $SELDON_HOME/logs/base $CATALINA_HOME/logs/seldon-server/base  && \
    ln -s $SELDON_HOME/logs/tomcat $CATALINA_HOME/logs/tomcat  && \
    ln -s $SELDON_HOME/logs/actions $CATALINA_HOME/logs/seldon-server/actions  )

ENV SELDON_ZKSERVERS zookeeper

ADD scripts/server.xml $CATALINA_HOME/conf/server.xml
ADD scripts/context.xml $CATALINA_HOME/conf/context.xml
ADD scripts/catalina.policy $CATALINA_HOME/conf/catalina.policy
ADD scripts/start-server.sh $SELDON_HOME/start-server.sh
ADD scripts/start-server-debug.sh $SELDON_HOME/start-server-debug.sh

WORKDIR ${SELDON_HOME}

# Define default command.
CMD ["bash"]

