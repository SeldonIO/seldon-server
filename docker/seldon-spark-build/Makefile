SELDON_SPARK_BUILD_IMAGE_VERSION=00
IMAGE_NAME=seldon-spark-build

build_image: seldon-server.tar.gz
	docker build --force-rm=true -t seldonio/$(IMAGE_NAME):$(SELDON_SPARK_BUILD_IMAGE_VERSION) .

seldon-server.tar.gz:
	@cd ../../.. && \
		tar -cvf seldon-server.tar seldon-server && gzip -v seldon-server.tar
	@mv -v ../../../seldon-server.tar.gz .

clean:
	@rm -fv seldon-server.tar.gz

