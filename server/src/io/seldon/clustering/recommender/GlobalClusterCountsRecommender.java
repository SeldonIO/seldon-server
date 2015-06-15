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

package io.seldon.clustering.recommender;

import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 10/12/14
 *         Time: 13:35
 */
@Component
public class GlobalClusterCountsRecommender implements ItemRecommendationAlgorithm {
	private static final String name = GlobalClusterCountsRecommender.class.getSimpleName();
    private static Logger logger = Logger.getLogger(GlobalClusterCountsRecommender.class.getName());
    private static final String DECAY_RATE_OPTION_NAME = "io.seldon.algorithm.clusters.decayratesecs";
    
    @Autowired
    JdoCountRecommenderUtils cUtils;
    
    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, Set<Integer> dimensions, int maxRecsCount,
                                                 RecommendationContext ctxt, List<Long> recentItemInteractions) {
        CountRecommender r = cUtils.getCountRecommender(client);
        if (r != null)
        {
            long t1 = System.currentTimeMillis();
            //RECENT ACTIONS
            Set<Long> exclusions = Collections.emptySet();
            if(ctxt.getMode()== RecommendationContext.MODE.EXCLUSION){
                exclusions = ctxt.getContextItems();
            }
            Double decayRate = ctxt.getOptsHolder().getDoubleOption(DECAY_RATE_OPTION_NAME);
            Map<Long, Double> recommendations = r.recommendGlobal(dimensions, maxRecsCount, exclusions, decayRate, null);
            long t2 = System.currentTimeMillis();
            logger.debug("Recommendation via cluster counts for user "+user+" took "+(t2-t1));
            List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
            for (Map.Entry<Long, Double> entry : recommendations.entrySet()){
                results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
            }
            return new ItemRecommendationResultSet(results, name);
        } else {
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
        }
    }

    @Override
    public String name() {
        return name;
    }
}
