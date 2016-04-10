#!/bin/bash

set -o nounset
set -o errexit

function get_data {
    
    echo "nameserver 8.8.8.8" >> /etc/resolv.conf
    wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
    unzip ml-100k.zip
    iconv -f iso-8859-1 -t utf-8 ml-100k/u.item -o ml-100k/u.item.utf8

}

function create_csv {

    echo "create items csv"
    cat <(echo 'id,title,release,url') <(cat ml-100k/u.item.utf8 | awk -F '|' '{printf("%d,\"%s\",\"%s\",\"%s\"\n",$1,$2,$3,$5)}') > items.csv

    echo "create users csv"
    cat <(echo "id") <(cat ml-100k/u.user | cut -d'|' -f1) > users.csv

    echo "create actions csv"
    cat <(echo "user_id,item_id,value,time") <(cat ml-100k/ua.base | cut -f1,2,3,4 --output-delimiter=,) > actions.csv

}

function setup_client {

    seldon-cli client --action setup --client-name ml100k --db-name ClientDB
    seldon-cli attr --action apply --client-name ml100k --json attr.json
    seldon-cli import --action items --client-name ml100k --file-path items.csv
    seldon-cli import --action users --client-name ml100k --file-path users.csv
    seldon-cli import --action actions --client-name ml100k --file-path actions.csv
}

function build_model {

    seldon-cli model --action add --client-name ml100k --model-name matrix-factorization
    seldon-cli model --action train --client-name ml100k --model-name matrix-factorization
}

function configure_runtime_scorer {

    seldon-cli rec_alg --action delete --client-name ml100k --recommender-name recentItemsRecommender
    seldon-cli rec_alg --action add --client-name ml100k --recommender-name recentMfRecommender
    seldon-cli rec_alg --action commit --client-name ml100k
}

function create_recommender {

    STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
    cd ${STARTUP_DIR}
    
    get_data
    
    create_csv

    setup_client

    build_model

    configure_runtime_scorer
}


create_recommender
