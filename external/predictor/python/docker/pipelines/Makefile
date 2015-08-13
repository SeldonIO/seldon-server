VERSION=1.0
IMAGE=pipelines

build:
	rm -fr python-modules
	cp -R ../../seldon python-modules
	docker build -t ${IMAGE} .

clean:
	rm -rf ./python-modules


