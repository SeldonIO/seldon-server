VERSION=1.0
IMAGE=vw_runtime

build:
	rm -rf test_vw
	cp -r ../../vw test_vw
	docker build -t ${IMAGE} .


run_interactive:
	docker run --name="vw_runtime" --rm -it --rm vw_runtime bash

run_dev:
	docker run --name="vw_runtime" --rm -it -p 5000:5000 -v /seldon-models:/seldon-models -v ${PWD}/scripts:/vw/vw_runtime --rm vw_runtime bash

