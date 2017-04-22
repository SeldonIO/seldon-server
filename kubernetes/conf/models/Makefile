SHELL=/bin/bash

.PHONY: matrix-factorization

#DATA_VOLUME="hostPath": {"path": "/seldon-data"}
DATA_VOLUME="glusterfs": {"endpoints": "glusterfs-cluster","path": "gv0","readOnly": false}

CLIENT=ml100k
LUIGI_CONF_PATH=/seldon-data/luigi/${CLIENT}.cfg
DAY=1

jobs/matrix-factorization-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	cat matrix-factorization.json.in | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' | sed -e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' | sed -e 's|%LUIGI_CONF_PATH%|$(LUIGI_CONF_PATH)|' > jobs/matrix-factorization-${CLIENT}-${DAY}.json

jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	cat matrix-factorization-clusters.json.in | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' | sed -e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' | sed -e 's|%LUIGI_CONF_PATH%|$(LUIGI_CONF_PATH)|' > jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json

jobs/item-similarity-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	cat item-similarity.json.in | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' | sed -e 's|%DATA_VOLUME%|$(DATA_VOLUME)|'| sed -e 's|%LUIGI_CONF_PATH%|$(LUIGI_CONF_PATH)|' > jobs/item-similarity-${CLIENT}-${DAY}.json


jobs/stream-itemsim-create-${CLIENT}.json:
	mkdir -p jobs
	cat stream-itemsim-create.json | sed -e "s|%CLIENT%|${CLIENT}|" | sed -e 's|%DATA_VOLUME%|$(DATA_VOLUME)|'  > jobs/stream-itemsim-create-${CLIENT}.json

jobs/stream-itemsim-dbupload-${CLIENT}.json:
	cat stream-itemsim-dbupload.json | sed -e "s|%CLIENT%|${CLIENT}|" | sed -e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' > jobs/stream-itemsim-dbupload-${CLIENT}.json


matrix-factorization:jobs/matrix-factorization-${CLIENT}-${DAY}.json

matrix-factorization-clusters:jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json

item-similarity:jobs/item-similarity-${CLIENT}-${DAY}.json

streaming-itemsim:jobs/stream-itemsim-create-${CLIENT}.json jobs/stream-itemsim-dbupload-${CLIENT}.json

clean_jobs:
	rm jobs/*
