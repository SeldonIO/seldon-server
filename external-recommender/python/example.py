#!/usr/bin/env python

import recommender_base
import random
import operator

def alg(
        user_id,
        item_id,
        client,
        recent_interactions_list,
        data_set,
        limit
        ):

    scores = {}
    for i in data_set:
        scores[i]=random.randint(0,100)

    sorted_scores=sorted(scores.items(), key=operator.itemgetter(1))
    sorted_scores = sorted_scores[::-1]
    top_scores = sorted_scores[0:limit]
    recs = dict(top_scores)

    return recs

if __name__ == "__main__":
    recommender_base.run({
        "alg" : alg,
        "memcache": {
            "host": "192.168.59.103",
            "port": 11211
        }
    })

