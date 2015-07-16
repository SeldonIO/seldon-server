IMAGE=semantic-vectors-for-seldon
VERSION=1.4

IMAGE_NAME=$(IMAGE)

semantic-vectors-lucene-tools:
	git clone https://github.com/SeldonIO/semantic-vectors-lucene-tools

semantic-vectors-lucene-tools/target/semvec-lucene-tools-${VERSION}-jar-with-dependencies.jar:semantic-vectors-lucene-tools
	cd semantic-vectors-lucene-tools ; git checkout tags/v${VERSION} ; mvn -DskipTests=true clean package ; cd ..

scripts/semvec-lucene-tools-${VERSION}-jar-with-dependencies.jar:semantic-vectors-lucene-tools/target/semvec-lucene-tools-${VERSION}-jar-with-dependencies.jar
	cp semantic-vectors-lucene-tools/target/semvec-lucene-tools-${VERSION}-jar-with-dependencies.jar scripts
	cd scripts ; ln -s semvec-lucene-tools-${VERSION}-jar-with-dependencies.jar semvec-lucene-tools.jar ; cd ..

build_image: scripts/semvec-lucene-tools-${VERSION}-jar-with-dependencies.jar
	docker build -t ${IMAGE} .


run-shell:
	docker run --name="testing_semvec" -t -i --rm --volumes-from general_logs --volumes-from seldon_data --link mysql:mysql --link consul:consul --link zookeeper:zookeeper -e "ZOOKEEPER_HOST=zookeeper" ${IMAGE} bash

tag:
	docker tag -f $(IMAGE_NAME) seldonio/$(IMAGE_NAME):$(VERSION)
	docker tag -f $(IMAGE_NAME) seldonio/$(IMAGE_NAME):latest

push:
	docker push $(PRIVATE_REGISTRY_HOST):$(PRIVATE_REGISTRY_PORT)/$(IMAGE_NAME):$(VERSION)

clean:
	@rm -f scripts/semvec-lucene-tools-${VERSION}-jar-with-dependencies.jar
	@rm -f scripts/semvec-lucene-tools.jar
	@rm -rf semantic-vectors-lucene-tools/


### tests

test:
	docker run --name "semvec" -rm  --net="host" semantic-vectors-for-seldon bash -c "./semvec/semantic-vectors.py --client movielens --zookeeper 127.0.0.1:2181 --activate --outputPath /seldon-models"



