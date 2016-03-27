
IMAGE_VERSION=`grep '\#VERSION' Dockerfile | cut -d= -f2`

create_spark_jar:
	docker run --name "seldon_spark_build" --rm -v ${PWD}/seldon-control:/mnt/build seldonio/seldon-spark-build:0.1 bash -c "cp /home/seldon/seldon-server/offline-jobs/spark/target/*-with-dependencies.jar /mnt/build"

build_image:
	cd ${IMAGE_FOLDER} ; docker build --force-rm=true -t seldonio/$(IMAGE_FOLDER):$(IMAGE_VERSION) .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push seldonio/$(IMAGE_FOLDER):$(IMAGE_VERSION)


