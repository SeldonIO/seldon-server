SHELL=/bin/bash

.PHONY: build_image push_to_dockerhub

SPARK_VERSION=1.3_v1
VERSION=1.3_v6
SELDON_VERSION=1.3

seldon-spark-${SELDON_VERSION}-jar-with-dependencies.jar:
	docker run --name "seldon_spark_build" --rm -v ${PWD}:/mnt/build seldonio/seldon-spark-build:${SPARK_VERSION} bash -c "cp /home/seldon/seldon-server/offline-jobs/spark/target/seldon-spark-${SELDON_VERSION}-jar-with-dependencies.jar /mnt/build"


seldon.conf:seldon.conf.in
	cat seldon.conf.in | sed 's/__SELDON_VERSION__/${SELDON_VERSION}/' > seldon.conf

build_image:seldon-spark-${SELDON_VERSION}-jar-with-dependencies.jar seldon.conf
	docker build --force-rm=true -t seldonio/seldon-control:${VERSION} .

push_to_dockerhub:
	docker login -u seldonio && \
		docker push seldonio/seldon-control:$(VERSION)


clean:
	rm -f seldon-spark-*.jar
	rm -f seldon.conf
