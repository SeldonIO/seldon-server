#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

if [ "$#" -eq 7 ]; then
    CLIENT=$1
    BUCKET=$2
    START_DAY=$3
    DAYS=$4
    AWS_KEY=$5
    AWS_SECRET=$6
    MEM_PERCENTAGE=$7

    python create_corpus.py --client ${CLIENT} --bucket ${BUCKET} --aws_key ${AWS_KEY} --aws_secret ${AWS_SECRET} --startDay ${START_DAY} --numDays ${DAYS}  --corpus data.corpus
    /kenlm/bin/lmplz -o 3 --prune 0 1 1 -S ${MEM_PERCENTAGE}%  < data.corpus > model.arpa
    python build_recommender.py --aws_key ${AWS_KEY} --aws_secret ${AWS_SECRET} --arpa model.arpa --dst s3://${BUCKET}/${CLIENT}/ngram/${START_DAY}
fi

if [ "$#" -eq 5 ]; then
    CLIENT=$1
    BUCKET=$2
    START_DAY=$3
    DAYS=$4
    MEM_PERCENTAGE=$5

    python create_corpus.py --client ${CLIENT} --bucket ${BUCKET} --startDay ${START_DAY} --numDays ${DAYS}  --corpus data.corpus
    /kenlm/bin/lmplz -o 3 --prune 0 1 1 -S ${MEM_PERCENTAGE}%  < data.corpus > model.arpa
    python build_recommender.py --arpa model.arpa --dst s3://${BUCKET}/${CLIENT}/ngram/${START_DAY}
fi

