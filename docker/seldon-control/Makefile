SHELL=/bin/bash

.PHONY: build_image push_to_dockerhub

SELDON_SPARK_BUILD_IMAGE_VERSION=00
SELDON_CONTROL_IMAGE_VERSION=00
SELDON_SPARK_VERSION=00
SELDON_PYTHON_PACKAGE_VERSION=00

Dockerfile: Dockerfile.in
	@cat Dockerfile.in | sed 's/%SELDON_PYTHON_PACKAGE_VERSION%/$(SELDON_PYTHON_PACKAGE_VERSION)/' > Dockerfile

seldon-spark-${SELDON_SPARK_VERSION}-jar-with-dependencies.jar:
	docker run --name "seldon_spark_build" --rm -v ${PWD}:/mnt/build seldonio/seldon-spark-build:${SELDON_SPARK_BUILD_IMAGE_VERSION} bash -c "cp /home/seldon/seldon-spark-${SELDON_SPARK_VERSION}-jar-with-dependencies.jar /mnt/build"

seldon.conf:seldon.conf.in
	cat seldon.conf.in | sed 's/__SELDON_VERSION__/${SELDON_SPARK_VERSION}/' > seldon.conf

seldon-server.tar.gz:
	@cd ../../.. && \
		tar --exclude='seldon-server/build/*' -cvf seldon-server.tar seldon-server && gzip -v seldon-server.tar
	@mv -v ../../../seldon-server.tar.gz .

build_image: Dockerfile seldon-server.tar.gz seldon-spark-${SELDON_SPARK_VERSION}-jar-with-dependencies.jar seldon.conf
	docker build --build-arg SELDON_SPARK_VERSION=${SELDON_SPARK_VERSION} --force-rm=true -t seldonio/seldon-control:${SELDON_CONTROL_IMAGE_VERSION} .

push_to_dockerhub:
	docker login -u seldonio && \
		docker push seldonio/seldon-control:$(SELDON_CONTROL_IMAGE_VERSION)


clean:
	rm -f Dockerfile
	rm -f seldon-spark-*.jar
	rm -f seldon.conf
	rm -f seldon-server.tar.gz

