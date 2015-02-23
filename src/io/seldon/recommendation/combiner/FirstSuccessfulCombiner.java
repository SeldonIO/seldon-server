/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.recommendation.combiner;

import io.seldon.clustering.recommender.ItemRecommendationResultSet;

import java.util.List;

/**
 *
 * Yields the results from the first alg that produces enough results.
 *
 * @author firemanphil
 *         Date: 23/02/15
 *         Time: 10:37
 */
public class FirstSuccessfulCombiner implements AlgorithmResultsCombiner {
    @Override
    public boolean isEnoughResults(int numRecsRequired, List<ItemRecommendationResultSet> resultsSets) {
        for (ItemRecommendationResultSet set : resultsSets){
            if (set.getResults().size() >= numRecsRequired)
                return true;
        }
        return false;
    }

    @Override
    public ItemRecommendationResultSet combine(int numRecsRequired, List<ItemRecommendationResultSet> resultsSets) {
        for (ItemRecommendationResultSet set : resultsSets){
            if(set.getResults().size() >= numRecsRequired)
                return set;
        }
        if(resultsSets.isEmpty()){
            return new ItemRecommendationResultSet();
        } else {
            return resultsSets.get(0);
        }

    }
}
