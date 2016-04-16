SHELL=/bin/bash

.PHONY: matrix-factorization

CLIENT=ml100k
DAY=1

jobs/matrix-factorization-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	cat matrix-factorization.json.in | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' > jobs/matrix-factorization-${CLIENT}-${DAY}.json

jobs/item-similarity-${CLIENT}-${DAY}.json:	
	mkdir -p jobs
	cat item-similarity.json.in | sed -e 's|%CLIENT%|$(CLIENT)|' | sed -e 's|%DAY%|$(DAY)|' > jobs/item-similarity-${CLIENT}-${DAY}.json


matrix-factorization:jobs/matrix-factorization-${CLIENT}-${DAY}.json

item-similarity:jobs/item-similarity-${CLIENT}-${DAY}.json



clean_jobs:
	rm jobs/*
