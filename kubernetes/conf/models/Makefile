SHELL=/bin/bash

.PHONY: matrix-factorization

CLIENT=ml100k
DAY=1

jobs/matrix-factorization-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	cat matrix-factorization.json.in | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' > jobs/matrix-factorization-${CLIENT}-${DAY}.json

jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	cat matrix-factorization-clusters.json.in | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' > jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json

jobs/item-similarity-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	cat item-similarity.json.in | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' > jobs/item-similarity-${CLIENT}-${DAY}.json


jobs/stream-itemsim-create-${CLIENT}.json:
	mkdir -p jobs
	cat stream-itemsim-create.json | sed -e "s|%CLIENT%|${CLIENT}|" > jobs/stream-itemsim-create-${CLIENT}.json

jobs/stream-itemsim-dbupload-${CLIENT}.json:
	cat stream-itemsim-dbupload.json | sed -e "s|%CLIENT%|${CLIENT}|" > jobs/stream-itemsim-dbupload-${CLIENT}.json


matrix-factorization:jobs/matrix-factorization-${CLIENT}-${DAY}.json

matrix-factorization-clusters:jobs/matrix-factorization-clusters-${CLIENT}-${DAY}.json

item-similarity:jobs/item-similarity-${CLIENT}-${DAY}.json

streaming-itemsim:jobs/stream-itemsim-create-${CLIENT}.json jobs/stream-itemsim-dbupload-${CLIENT}.json

clean_jobs:
	rm jobs/*
