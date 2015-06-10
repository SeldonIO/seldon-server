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

/**
 * @author firemanphil
 *         Date: 11/12/14
 *         Time: 15:20
 */
public abstract class BaseItemClusterCountsRecommender {

    private static final String MIN_ITEMS_FOR_VALID_CLUSTER_OPTION_NAME
            = "io.seldon.algorithm.clusters.minnumberitemsforvalidclusterresult";
    private static final String DECAY_RATE_OPTION_NAME = "io.seldon.algorithm.clusters.decayratesecs";
    private static final String CLUSTER_ALG_OPTION_NAME = "io.seldon.algorithm.clusters.itemalg";

    private static Logger logger = Logger.getLogger( BaseItemClusterCountsRecommender.class.getName() );

    @Autowired
    JdoCountRecommenderUtils cUtils;
    
    ItemRecommendationResultSet recommend(String recommenderName, String recommenderType, String client, Long user, Set<Integer> dimensions, int maxRecsCount,
                                          RecommendationContext ctxt) {
        RecommendationContext.OptionsHolder optionsHolder = ctxt.getOptsHolder();
        if (ctxt.getCurrentItem() != null) {
            Set<Long> exclusions = Collections.emptySet();
            if (ctxt.getMode() == RecommendationContext.MODE.EXCLUSION) {
                exclusions = ctxt.getContextItems();
            }
            CountRecommender r = cUtils.getCountRecommender(client);
            if (r != null) {
                long t1 = System.currentTimeMillis();
                Integer minClusterItems = optionsHolder.getIntegerOption(MIN_ITEMS_FOR_VALID_CLUSTER_OPTION_NAME);
                Double decayRate = optionsHolder.getDoubleOption(DECAY_RATE_OPTION_NAME);
                String clusterAlgorithm = optionsHolder.getStringOption(CLUSTER_ALG_OPTION_NAME);
                Map<Long, Double> recommendations = r.recommendUsingItem(recommenderType,ctxt.getCurrentItem(), dimensions,
                        maxRecsCount, exclusions, decayRate, clusterAlgorithm, minClusterItems);
                long t2 = System.currentTimeMillis();
                logger.debug("Recommendation via cluster counts for item  " + ctxt.getCurrentItem() + " for user " + user + " took " + (t2 - t1));
                List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
                for (Map.Entry<Long, Double> entry : recommendations.entrySet()) {
                    results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
                }
                return new ItemRecommendationResultSet(results, recommenderName);
            } else {
                return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), recommenderName);
            }


        } else {
            logger.info("Can't cluster count for item for user " + user  + " as no current item passed in");
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), recommenderName);
        }
    }

}
