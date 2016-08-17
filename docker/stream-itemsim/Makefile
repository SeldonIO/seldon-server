SHELL=/bin/bash

.PHONY: build_image push_to_dockerhub

SELDON_STREAM_BUILD_IMAGE_VERSION=00
SELDON_STREAM_ITEMSIM_IMAGE_VERSION=00
SELDON_STREAM_VERSION=00


seldon-stream-${SELDON_STREAM_VERSION}-jar-with-dependencies.jar:
	docker run --name "seldon_stream_build" --rm -v ${PWD}:/mnt/build seldonio/seldon-stream:${SELDON_STREAM_BUILD_IMAGE_VERSION} bash -c "cp /home/seldon/seldon-stream-${SELDON_STREAM_VERSION}-jar-with-dependencies.jar /mnt/build"


build_image: seldon-stream-${SELDON_STREAM_VERSION}-jar-with-dependencies.jar 
	docker build --build-arg SELDON_STREAM_VERSION=${SELDON_STREAM_VERSION} --force-rm=true -t seldonio/seldon-stream-itemsim:${SELDON_STREAM_ITEMSIM_IMAGE_VERSION} .

push_to_dockerhub:
	docker login -u seldonio && \
		docker push seldonio/seldon-stream-itemsim:$(SELDON_STREAM_ITEMSIM_IMAGE_VERSION)


clean:
	rm -f seldon-stream-*.jar
	rm -f seldon-server.tar.gz

