IMAGE_VERSION=00
IMAGE_NAME=seldon-spark-build

build_image: seldon-server.tar.gz
	docker build --force-rm=true -t seldonio/$(IMAGE_NAME):$(IMAGE_VERSION) .

seldon-server.tar.gz:
	@cd ../../.. && \
		tar --exclude='.git*' -cvf seldon-server.tar seldon-server && gzip -v seldon-server.tar
	@mv -v ../../../seldon-server.tar.gz .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push seldonio/$(IMAGE_NAME):$(IMAGE_VERSION)

clean:
	@rm -fv seldon-server.tar.gz

