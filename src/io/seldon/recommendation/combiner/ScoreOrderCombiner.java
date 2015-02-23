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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Combines results in score order as soon as there are enough results.
 *
 * @author firemanphil
 *         Date: 23/02/15
 *         Time: 10:20
 */
public class ScoreOrderCombiner implements AlgorithmResultsCombiner {
    @Override
    public boolean isEnoughResults(int numRecsRequired, List<ItemRecommendationResultSet> resultsSets) {
        int totalSize = 0;
        for (ItemRecommendationResultSet set : resultsSets){
            totalSize += set.getResults().size();
        }
        return totalSize >= numRecsRequired;
    }

    @Override
    public ItemRecommendationResultSet combine(int numRecsRequired, List<ItemRecommendationResultSet> resultsSets) {
        List<ItemRecommendationResultSet.ItemRecommendationResult> ordered = new ArrayList<>();
        for (ItemRecommendationResultSet set : resultsSets){
            ordered.addAll(set.getResults());
        }
        Collections.sort(ordered, Collections.reverseOrder());
        return new ItemRecommendationResultSet(ordered);
    }
}
