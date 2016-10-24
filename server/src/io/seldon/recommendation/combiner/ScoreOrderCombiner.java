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
import io.seldon.recommendation.RecommendationPeer;

import org.apache.commons.lang3.StringUtils;
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
    public boolean isEnoughResults(int numRecsRequired, List<RecommendationPeer.RecResultContext> resultsSets) {
        Set<ItemRecommendationResultSet.ItemRecommendationResult> uniqueItems = new HashSet<>();
        for (RecommendationPeer.RecResultContext set : resultsSets){
            uniqueItems.addAll(set.resultSet.getResults());
        }
        return uniqueItems.size() >= numRecsRequired;

    }

    @Override
    public RecommendationPeer.RecResultContext combine(int numRecsRequired, List<RecommendationPeer.RecResultContext> resultsSets) {
        Map<Long,String> item_recommender_lookup = new HashMap<>();
        List<String> validAlgs = new ArrayList<>();
        Map<ItemRecommendationResultSet.ItemRecommendationResult, Float> scores = new HashMap<>();
        List<ItemRecommendationResultSet.ItemRecommendationResult> ordered = new ArrayList<>();
        for (RecommendationPeer.RecResultContext set : resultsSets){
            if(set.resultSet.getResults().size() > 0)
                validAlgs.add(set.algKey);
            for (ItemRecommendationResultSet.ItemRecommendationResult itemRecommendationResult : set.resultSet.getResults()) {
                Float previousResult = scores.get(itemRecommendationResult);
                if(previousResult!=null){
                    if(previousResult< itemRecommendationResult.score)
                        scores.put(itemRecommendationResult, itemRecommendationResult.score);
                        capture_recommender_used_for_item(item_recommender_lookup, itemRecommendationResult, set);
                } else {
                    scores.put(itemRecommendationResult, itemRecommendationResult.score);
                    capture_recommender_used_for_item(item_recommender_lookup, itemRecommendationResult, set);
                }
            }
        }
        ordered.addAll(scores.keySet());
        Collections.sort(ordered, Collections.reverseOrder());
        RecommendationPeer.RecResultContext recResultContext = new RecommendationPeer.RecResultContext(new ItemRecommendationResultSet(ordered, StringUtils.join(validAlgs, ':')), StringUtils.join(validAlgs, ':'));
        recResultContext.item_recommender_lookup = item_recommender_lookup;
        return recResultContext;
    }
    
    private static void capture_recommender_used_for_item(Map<Long,String> item_recommender_lookup, ItemRecommendationResultSet.ItemRecommendationResult itemRecommendationResult, RecommendationPeer.RecResultContext recResultContext) {
        String original_value = item_recommender_lookup.put(itemRecommendationResult.item, recResultContext.resultSet.getRecommenderName());
        if (original_value != null) {
            item_recommender_lookup.put(itemRecommendationResult.item, original_value);
        }
    }
}
