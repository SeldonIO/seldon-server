SHELL=/bin/bash

SELDON_HOME := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../../..)
BUILD_VERSIONS_GENERATED_FILE=$(SELDON_HOME)/build/build_versions_generated

.PHONY: matrix-factorization


CLIENT=ml100k
LUIGI_CONF_PATH=/seldon-data/luigi/${CLIENT}.cfg
DAY=1

jobs/matrix-factorization-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat model.json.in | sed -e 's|%NAME%|matrix-factorization-clusters|' | sed -e 's|%LUIGI_TASK%|SeldonMatrixFactorization|' | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' | sed -e 's|%LUIGI_CONF_PATH%|$(LUIGI_CONF_PATH)|' | sed -e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" > jobs/matrix-factorization-${CLIENT}-${DAY}.json


jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat model.json.in | sed -e 's|%NAME%|matrix-factorization-clusters|'  | sed -e 's|%LUIGI_TASK%|SeldonMatrixFactorizationClusters|' | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' | sed -e 's|%LUIGI_CONF_PATH%|$(LUIGI_CONF_PATH)|' | sed -e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" > jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json


jobs/item-similarity-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat model.json.in | sed -e 's|%NAME%|item-similarity|' | sed -e 's|%LUIGI_TASK%|SeldonItemSimilarity|' | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' | sed -e 's|%LUIGI_CONF_PATH%|$(LUIGI_CONF_PATH)|' | sed -e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" > jobs/item-similarity-${CLIENT}-${DAY}.json

jobs/most-popular-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat model.json.in | sed -e 's|%NAME%|most-popular|' | sed -e 's|%LUIGI_TASK%|SeldonMostPopular|' | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' | sed -e 's|%LUIGI_CONF_PATH%|$(LUIGI_CONF_PATH)|' | sed -e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" > jobs/most-popular-${CLIENT}-${DAY}.json

jobs/most-popular-dim-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat model.json.in | sed -e 's|%NAME%|most-popular-dim|' | sed -e 's|%LUIGI_TASK%|SeldonMostPopularDim|' | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' | sed -e 's|%LUIGI_CONF_PATH%|$(LUIGI_CONF_PATH)|' | sed -e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" > jobs/most-popular-dim-${CLIENT}-${DAY}.json


jobs/stream-itemsim-create-${CLIENT}.json:
	mkdir -p jobs
	cat stream-itemsim-create.json | sed -e "s|%CLIENT%|${CLIENT}|"  > jobs/stream-itemsim-create-${CLIENT}.json

jobs/stream-itemsim-dbupload-${CLIENT}.json:
	cat stream-itemsim-dbupload.json | sed -e "s|%CLIENT%|${CLIENT}|" > jobs/stream-itemsim-dbupload-${CLIENT}.json


matrix-factorization:jobs/matrix-factorization-${CLIENT}-${DAY}.json

matrix-factorization-clusters:jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json

item-similarity:jobs/item-similarity-${CLIENT}-${DAY}.json

most-popular:jobs/most-popular-${CLIENT}-${DAY}.json

most-popular-dim:jobs/most-popular-dim-${CLIENT}-${DAY}.json

streaming-itemsim:jobs/stream-itemsim-create-${CLIENT}.json jobs/stream-itemsim-dbupload-${CLIENT}.json

clean_jobs:
	rm -f jobs/*
