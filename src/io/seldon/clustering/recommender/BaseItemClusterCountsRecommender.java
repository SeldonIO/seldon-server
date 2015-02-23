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
import io.seldon.similarity.item.ItemSimilarityRecommender;
import io.seldon.trust.impl.CFAlgorithm;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author firemanphil
 *         Date: 11/12/14
 *         Time: 15:20
 */
public abstract class BaseItemClusterCountsRecommender {

    private static Logger logger = Logger.getLogger( ItemSimilarityRecommender.class.getName() );

    ItemRecommendationResultSet recommend(CFAlgorithm options, Long user, int dimensionId, int maxRecsCount,
                                          RecommendationContext ctxt, CFAlgorithm.CF_RECOMMENDER recommenderType) {
        if (ctxt.getCurrentItem() != null) {
            Set<Long> exclusions = Collections.emptySet();
            if (ctxt.getMode() == RecommendationContext.MODE.EXCLUSION) {
                exclusions = ctxt.getContextItems();
            }
            JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
            CountRecommender r = cUtils.getCountRecommender(options.getName());
            if (r != null) {
                //change to significant version of cluster counts if needed
                if (recommenderType == CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_FOR_ITEM_SIGNIFICANT)
                    r.setRecommenderType(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_SIGNIFICANT);
                long t1 = System.currentTimeMillis();
                Map<Long, Double> recommendations = r.recommendUsingItem(ctxt.getCurrentItem(), dimensionId, maxRecsCount, exclusions, options.getDecayRateSecs(), options.getClusterAlgorithm(), options.getMinNumberItemsForValidClusterResult());
                long t2 = System.currentTimeMillis();
                logger.debug("Recommendation via cluster counts for item  " + ctxt.getCurrentItem() + " for user " + user + " took " + (t2 - t1));
                List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
                for (Map.Entry<Long, Double> entry : recommendations.entrySet()) {
                    results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
                }
                return new ItemRecommendationResultSet(results);
            } else {
                return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList());
            }


        } else {
            logger.info("Can't cluster count for item for user " + user  + " as no current item passed in");
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList());
        }
    }

}
