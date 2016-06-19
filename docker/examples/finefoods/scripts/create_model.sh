#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 2 ]; then
    echo "need <model_folder> <sample in range 0.0 to 1.0>"
    exit -1
fi

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
MODEL_FOLDER=$1
SAMPLE=$2

function get_data {
    
    cd ${STARTUP_DIR}
    echo "nameserver 8.8.8.8" >> /etc/resolv.conf
    wget http://snap.stanford.edu/data/finefoods.txt.gz
    gunzip finefoods.txt.gz
    iconv -f iso-8859-1 -t utf-8 finefoods.txt -o finefoods_utf8.txt
    mkdir -p data
    echo "sentiment,review" > data/train.csv
    cat finefoods_utf8.txt | awk 'BEGIN{OFS=","}/^review\/score/{if ($2<3){s=0} else if ($2>3){s=1}else{s=-1}}/^review\/text/{if (s>-1){gsub(/,/," ");print s,$0}}' | sed -e 's/review\/text://g' -e 's/<br \/>//g' -e 's/\./ /g' >> data/train.csv

}

function train {

    mkdir ${STARTUP_DIR}/folds
    python ${STARTUP_DIR}/create_model.py --data ${STARTUP_DIR}/data --model ${MODEL_FOLDER} --sample ${SAMPLE}
    perf -SEN -SPC -NPV -PPV -ACC -confusion -files folds/1_correct.txt <(cut -d' ' -f2 folds/1_predictions_proba.txt)
}


get_data

train
