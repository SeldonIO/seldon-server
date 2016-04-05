#VERSION=1.2.3
FROM maven:3.3.3-jdk-8

ENV CATALINA_HOME /usr/local/tomcat
ENV PATH $CATALINA_HOME/bin:$PATH
RUN mkdir -p "$CATALINA_HOME"
WORKDIR $CATALINA_HOME

# see https://www.apache.org/dist/tomcat/tomcat-8/KEYS
RUN set -ex \
	&& for key in \
		05AB33110949707C93A279E3D3EFE6B686867BA6 \
		07E48665A34DCAFAE522E5E6266191C37C037D42 \
		47309207D818FFD8DCD3F83F1931D684307A10A5 \
		541FBE7D8F78B25E055DDEE13C370389288584E7 \
		61B832AC2F1C5A90F0F9B00A1C506407564C17A3 \
		713DA88BE50911535FE716F5208B0AB1D63011C7 \
		79F7026C690BAA50B92CD8B66A3AD3F4F22C4FED \
		9BA44C2621385CB966EBA586F72C284D731FABEE \
		A27677289986DB50844682F8ACB77FC2E86E29AC \
		A9C5DF4D22E99998D9875A5110C01C5A2F6059E7 \
		DCFD35E0BF8CA7344752DE8B6FB21E8933C60243 \
		F3A04C595DB5B6A5F1ECA43E3B7BBB100D811BBE \
		F7DA48BB64BCB84ECBA7EE6935CD23C10D498E23 \
	; do \
		gpg --keyserver ha.pool.sks-keyservers.net --recv-keys "$key"; \
	done

ENV TOMCAT_MAJOR 7
ENV TOMCAT_VERSION 7.0.68
ENV TOMCAT_TGZ_URL https://www.apache.org/dist/tomcat/tomcat-$TOMCAT_MAJOR/v$TOMCAT_VERSION/bin/apache-tomcat-$TOMCAT_VERSION.tar.gz

RUN set -x \
	&& curl -fSL "$TOMCAT_TGZ_URL" -o tomcat.tar.gz \
	&& curl -fSL "$TOMCAT_TGZ_URL.asc" -o tomcat.tar.gz.asc \
	&& gpg --batch --verify tomcat.tar.gz.asc tomcat.tar.gz \
	&& tar -xvf tomcat.tar.gz --strip-components=1 \
	&& rm bin/*.bat \
	&& rm tomcat.tar.gz*

EXPOSE 8080

RUN apt-get update && \
    apt-get install -y less telnet emacs 

ENV SELDON_HOME /home/seldon
ENV SELDON_TAG v1.2.3
RUN mkdir -p  "$SELDON_HOME"
WORKDIR $SELDON_HOME
RUN (git clone https://github.com/SeldonIO/seldon-server --branch ${SELDON_TAG} && \
    cd seldon-server/server && \
    mvn package)

RUN (mkdir -p $SELDON_HOME/ROOT && \
    cp $SELDON_HOME/seldon-server/server/target/*.war $SELDON_HOME/ROOT && \
    cd $SELDON_HOME/ROOT && unzip *.war && rm *.war && \
    rm -rf $CATALINA_HOME/webapps/ROOT && ln -s $SELDON_HOME/ROOT $CATALINA_HOME/webapps/ROOT && \
    mkdir -p $SELDON_HOME/logs/base && \
    mkdir -p $SELDON_HOME/logs/actions && \
    mkdir -p $CATALINA_HOME/logs/seldon-server && \
    ln -s $SELDON_HOME/logs/base $CATALINA_HOME/logs/seldon-server/base  && \
    ln -s $SELDON_HOME/logs/actions $CATALINA_HOME/logs/seldon-server/actions  )

ENV SELDON_ZKSERVERS zookeeper

ADD scripts/server.xml $CATALINA_HOME/conf/server.xml
ADD scripts/context.xml $CATALINA_HOME/conf/context.xml
ADD scripts/start-server.sh $SELDON_HOME/start-server.sh

# Define default command.
CMD ["bash"]



