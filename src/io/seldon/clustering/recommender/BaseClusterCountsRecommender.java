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
import io.seldon.trust.impl.CFAlgorithm;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author firemanphil
 *         Date: 03/12/14
 *         Time: 14:14
 */
public class BaseClusterCountsRecommender {
    private static Logger logger = Logger.getLogger(BaseClusterCountsRecommender.class.getName());

    public ItemRecommendationResultSet recommend(CFAlgorithm.CF_RECOMMENDER recommenderType,
                                                 CFAlgorithm options, Long user, int dimensionId,
                                                 int maxRecsCount, RecommendationContext ctxt) {
        JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
        CountRecommender r = cUtils.getCountRecommender(options.getName());
        if (r != null)
        {
            long t1 = System.currentTimeMillis();
            r.setRecommenderType(recommenderType);
            Set<Long> exclusions = Collections.emptySet();
            if(ctxt.getMode()== RecommendationContext.MODE.EXCLUSION){
                exclusions = ctxt.getContextItems();
            }
            boolean includeShortTermClusters = recommenderType == CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_DYNAMIC;
            Map<Long, Double> recommendations = r.recommend(user, null, dimensionId, maxRecsCount, exclusions, includeShortTermClusters,
                    options.getLongTermWeight(), options.getShortTermWeight(), options.getDecayRateSecs(),
                    options.getMinNumberItemsForValidClusterResult());
            long t2 = System.currentTimeMillis();
            logger.debug("Recommendation via cluster counts for user "+user+" took "+(t2-t1)+" and got back "+recommendations.size()+" recommednations");
            List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
            for (Map.Entry<Long, Double> entry : recommendations.entrySet()){
                results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
            }
            return new ItemRecommendationResultSet(results);
        } else {
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList());
        }
    }
}
