SHELL=/bin/bash

.PHONY: build push

VERSION=1.2.3_v2
SELDON_VERSION=1.2.3

seldon-spark-${SELDON_VERSION}-jar-with-dependencies.jar:
	docker run --name "seldon_spark_build" --rm -v ${PWD}:/mnt/build seldonio/seldon-spark-build:${SELDON_VERSION} bash -c "cp /home/seldon/seldon-server/offline-jobs/spark/target/seldon-spark-${SELDON_VERSION}-jar-with-dependencies.jar /mnt/build"


seldon.conf:seldon.conf.in
	cat seldon.conf.in | sed 's/__SELDON_VERSION__/${SELDON_VERSION}/' > seldon.conf

build:seldon-spark-${SELDON_VERSION}-jar-with-dependencies.jar seldon.conf
	docker build --force-rm=true -t seldonio/seldon-control:${VERSION} .

push:
	docker login -u seldonio && \
		docker push seldonio/seldon-control:$(VERSION)


clean:
	rm -f seldon-spark-*.jar
	rm -f seldon.conf
