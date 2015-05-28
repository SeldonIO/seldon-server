CURRENT_DIR:=$(shell pwd)
COMMON_DIR:=$(CURRENT_DIR)/../../../common
include $(COMMON_DIR)/images.sh

IMAGE_NAME=$(SELDONIO_TDAGENT_IMAGE_NAME)
IMAGE_VERSION=$(SELDONIO_TDAGENT_IMAGE_VERSION)

build_image:
	docker build --force-rm=true -t $(IMAGE_NAME):$(IMAGE_VERSION) .

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE_NAME):$(IMAGE_VERSION)


