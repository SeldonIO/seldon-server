VERSION=1.0
IMAGE=seldonio/vw_runtime

build:
	rm -rf test_vw
	cp -r ../../vw test_vw
	docker build -t ${IMAGE} .


push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)

run_interactive:
	docker run --name="vw_runtime" --rm -it --rm ${IMAGE} bash


