#!/bin/bash

set -o nounset
set -o errexit

function get_data {
    
    echo "nameserver 8.8.8.8" >> /etc/resolv.conf
    wget http://files.grouplens.org/datasets/movielens/ml-10m.zip
    unzip ml-10m.zip
    iconv -f iso-8859-1 -t utf-8 ml-10M100K/movies.dat -o ml-10M100K/movies.dat.utf8

}

function create_csv {

    echo "create items csv"
    cat <(echo 'id,title') <(cat ml-10M100K/movies.dat.utf8 | awk -F '::' '{printf("%d,\"%s\"\n",$1,$2)}') > items.csv

    echo "create users csv"
    cat <(echo "id") <(cat ml-10M100K/ratings.dat | awk -F'::' '{print $1}' | uniq) > users.csv

    echo "create actions csv"
    cat <(echo "user_id,item_id,value,time") <(cat ml-10M100K/ratings.dat | awk -F'::' 'BEGIN{OFS=","}{print $1,$2,$3,$4}') > actions.csv

}

function setup_client {

    seldon-cli client --action setup --client-name ml10m --db-name ClientDB
    seldon-cli attr --action apply --client-name ml10m --json attr.json
    seldon-cli import --action items --client-name ml10m --file-path items.csv
    seldon-cli import --action users --client-name ml10m --file-path users.csv
    seldon-cli import --action actions --client-name ml10m --file-path actions.csv
}

function build_model_mf {

    rm -rf /seldon-data/seldon-models/ml10m/matrix-factorization/1
    luigi --module seldon.luigi.spark SeldonMatrixFactorization --local-schedule --client ml10m --startDay 1 

}

function build_model_isim {

    rm -rf /seldon-data/seldon-models/ml10m/item-similarity/1
    export DB_USER=`seldon-cli db --action show | grep user: | awk '{print $2}'`
    export DB_PASSWORD=`seldon-cli db --action show | grep password: | awk '{print $2}'`
    luigi --module seldon.luigi.spark SeldonItemSimilarity --local-schedule --client ml10m --startDay 1 --ItemSimilaritySparkJob-sample 0.25 --ItemSimilaritySparkJob-dimsumThreshold 0.5 --ItemSimilaritySparkJob-limit 100 --db-user ${DB_USER} --db-pass ${DB_PASSWORD}

}

function configure_runtime_scorer_mf {

    cat <<EOF | seldon-cli rec_alg --action create --client-name ml10m -f -
{
    "defaultStrategy": {
        "algorithms": [
            {
                "config": [
                    {
                        "name": "io.seldon.algorithm.general.numrecentactionstouse",
                        "value": "1"
                    }
                ],
                "filters": [],
                "includers": [],
                "name": "recentMfRecommender"
            }
        ],
        "combiner": "firstSuccessfulCombiner",
        "diversityLevel": 3
    },
    "recTagToStrategy": {}
}
EOF
    seldon-cli rec_alg --action commit --client-name ml10m
    #pull updated conf from zookeeper so its safe
    seldon-cli client --action zk_pull
}

function configure_runtime_scorer_isim {

    cat <<EOF | seldon-cli rec_alg --action create --client-name ml10m -f -
{
    "defaultStrategy": {
        "algorithms": [
            {
                "config": [
                    {
                        "name": "io.seldon.algorithm.general.numrecentactionstouse",
                        "value": "1"
                    }
                ],
                "filters": [],
                "includers": [],
                "name": "itemSimilarityRecommender"
            }
        ],
        "combiner": "firstSuccessfulCombiner",
        "diversityLevel": 3
    },
    "recTagToStrategy": {}
}
EOF
    seldon-cli rec_alg --action commit --client-name ml10m
    #pull updated conf from zookeeper so its safe
    seldon-cli client --action zk_pull
}

function create_recommender {

    STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
    cd ${STARTUP_DIR}
    
    get_data
    
    create_csv

    setup_client

    if [[ "$RECOMENDER" == "item-similarity" ]]; then
	build_model_isim
	configure_runtime_scorer_isim
    fi
    if [[ "$RECOMENDER" == "matrix-factorization" ]]; then
	build_model_mf
	configure_runtime_scorer_mf
    fi
}

if [ "$#" -ne 1 ]; then
    echo "need item-similarity|matrix-factorization"
    exit -1
fi

RECOMENDER=$1
create_recommender

