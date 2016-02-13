VERSION=1.0
IMAGE=seldonio/microservices

build:
	docker build -t ${IMAGE} .

shell:
	docker run -it --rm ${IMAGE} bash

tag:
	docker tag -f $(IMAGE) $(IMAGE):$(VERSION)

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)


test_start_aws_recommender:
	docker run -it --rm -p 5000:5000 ${IMAGE} bash -c "cd scripts ; python start_recommender.py --src ${RECOMMENDER_S3_LOC} --aws_key ${AWS_KEY} --aws_secret ${AWS_SECRET}"



