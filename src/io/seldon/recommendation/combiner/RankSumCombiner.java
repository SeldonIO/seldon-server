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
import io.seldon.trust.impl.jdo.RecommendationPeer;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 *
 * Combine recommendations from algorithms based on their rank in the list of recs.
 *
 * @author firemanphil
 *         Date: 23/02/15
 *         Time: 11:00
 */
@Component
public class RankSumCombiner implements AlgorithmResultsCombiner{

    private final int numResultSetsToUse;

    @Autowired
    public RankSumCombiner(@Value("${combiner.maxResultSets:2}") int numResultSetsToUse){
        this.numResultSetsToUse = numResultSetsToUse;
    }


    @Override
    public boolean isEnoughResults(int numRecsRequired, List<RecommendationPeer.RecResultContext> resultsSets) {
        int numValidSets = 0;
        for (RecommendationPeer.RecResultContext set : resultsSets){
            if (set.resultSet.getResults().size() >= numRecsRequired){
                numValidSets++;
            }
        }
        return numValidSets >= numResultSetsToUse;
    }

    @Override
    public RecommendationPeer.RecResultContext combine(int numRecsRequired, List<RecommendationPeer.RecResultContext> resultsSets) {
        Map<ItemRecommendationResultSet.ItemRecommendationResult, Integer> rankSumMap = new HashMap<>();
        List<RecommendationPeer.RecResultContext> validResultSets = new ArrayList<>();
        List<String> validResultsAlgKeys = new ArrayList<>();
        for (RecommendationPeer.RecResultContext set : resultsSets){
            if(set.resultSet.getResults().size() >= numRecsRequired) {
                validResultSets.add(set);
                validResultsAlgKeys.add(set.algKey);
            }
        }

        for (int i = 0; i < numRecsRequired; i++){
            for (RecommendationPeer.RecResultContext validResultSet : validResultSets) {
                List<ItemRecommendationResultSet.ItemRecommendationResult> ordered = validResultSet.resultSet.getResults();
                Collections.sort(ordered, Collections.reverseOrder());
                Integer rankSum = rankSumMap.get(ordered.get(i));
                if(rankSum == null) rankSum = 0;
                rankSum += (numRecsRequired -i);
                rankSumMap.put(ordered.get(i), rankSum);
            }
        }

        List<ItemRecommendationResultSet.ItemRecommendationResult> orderedResults = new ArrayList<>();
        for (Map.Entry<ItemRecommendationResultSet.ItemRecommendationResult, Integer> entry : rankSumMap.entrySet()) {
            Float newScore = entry.getValue().floatValue();
            Long item = entry.getKey().item;
            ItemRecommendationResultSet.ItemRecommendationResult result = new ItemRecommendationResultSet.ItemRecommendationResult(item, newScore);
            orderedResults.add(result);
        }

        Collections.sort(orderedResults, Collections.reverseOrder());
        return new RecommendationPeer.RecResultContext(new ItemRecommendationResultSet(orderedResults), StringUtils.join(validResultsAlgKeys,':'));
    }

}
