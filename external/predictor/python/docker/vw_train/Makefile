VERSION=1.0
IMAGE=vw_train

build:
	rm -r python-modules
	cp -R ../../seldon python-modules
	docker build -t ${IMAGE} .

clean:
	rm -rf ./python-modules


run_interactive:
	docker run --name="vw_train" -it --net=host --rm vw_train bash

