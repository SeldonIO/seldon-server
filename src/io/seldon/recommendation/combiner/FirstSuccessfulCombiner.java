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
import org.springframework.stereotype.Component;

import java.util.List;

/**
 *
 * Yields the results from the first alg that produces enough results.
 *
 * @author firemanphil
 *         Date: 23/02/15
 *         Time: 10:37
 */
@Component
public class FirstSuccessfulCombiner implements AlgorithmResultsCombiner {
    @Override
    public boolean isEnoughResults(int numRecsRequired, List<RecommendationPeer.RecResultContext> resultsSets) {
        for (RecommendationPeer.RecResultContext set : resultsSets){
            if (set.resultSet.getResults().size() >= numRecsRequired)
                return true;
        }
        return false;
    }

    @Override
    public RecommendationPeer.RecResultContext combine (
            int numRecsRequired, List<RecommendationPeer.RecResultContext> resultsSets) {
        int maxSize = 0;
        RecommendationPeer.RecResultContext maxSizedResults = null;
        for (RecommendationPeer.RecResultContext recsContext : resultsSets){
            int size = recsContext.resultSet.getResults().size();
            if(size >= numRecsRequired) {
                return recsContext;
            } else if(size > maxSize) {
                maxSize = size;
                maxSizedResults = recsContext;
            }

        }
        if(maxSizedResults==null){
            return RecommendationPeer.RecResultContext.EMPTY;
        } else {
            return maxSizedResults;
        }

    }
}
