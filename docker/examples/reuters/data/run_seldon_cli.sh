#!/bin/bash

set -o nounset
set -o errexit


seldon-cli client --action setup --client-name reuters --db-name ClientDB

seldon-cli attr --action apply --client-name reuters --json attr.json

seldon-cli import --action items --client-name reuters --file-path reuters-21578.csv 



