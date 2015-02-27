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
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Combines results in score order as soon as there are enough results.
 *
 * @author firemanphil
 *         Date: 23/02/15
 *         Time: 10:20
 */
@Component
public class ScoreOrderCombiner implements AlgorithmResultsCombiner {
    @Override
    public boolean isEnoughResults(int numRecsRequired, List<ItemRecommendationResultSet> resultsSets) {
        Set<ItemRecommendationResultSet.ItemRecommendationResult> uniqueItems = new HashSet<>();
        for (ItemRecommendationResultSet set : resultsSets){
            uniqueItems.addAll(set.getResults());
        }
        return uniqueItems.size() >= numRecsRequired;

    }

    @Override
    public ItemRecommendationResultSet combine(int numRecsRequired, List<ItemRecommendationResultSet> resultsSets) {

        Map<ItemRecommendationResultSet.ItemRecommendationResult, Float> scores = new HashMap<>();
        List<ItemRecommendationResultSet.ItemRecommendationResult> ordered = new ArrayList<>();
        for (ItemRecommendationResultSet set : resultsSets){
            for (ItemRecommendationResultSet.ItemRecommendationResult itemRecommendationResult : set.getResults()) {
                Float previousResult = scores.get(itemRecommendationResult);
                if(previousResult!=null){
                    if(previousResult< itemRecommendationResult.score)
                        scores.put(itemRecommendationResult, itemRecommendationResult.score);
                }
                scores.put(itemRecommendationResult, itemRecommendationResult.score);
            }
        }
        ordered.addAll(scores.keySet());
        Collections.sort(ordered, Collections.reverseOrder());
        return new ItemRecommendationResultSet(ordered);
    }
}
