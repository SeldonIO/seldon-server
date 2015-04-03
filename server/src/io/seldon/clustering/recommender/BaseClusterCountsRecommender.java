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
 *         Date: 03/12/14
 *         Time: 14:14
 */
public class BaseClusterCountsRecommender {
    private static final String LONG_TERM_WEIGHT_OPTION_NAME = "io.seldon.algorithm.clusters.longtermweight";
    private static final String SHORT_TERM_WEIGHT_OPTION_NAME = "io.seldon.algorithm.clusters.shorttermweight";
    private static final String MIN_ITEMS_FOR_VALID_CLUSTER_OPTION_NAME 
            = "io.seldon.algorithm.clusters.minnumberitemsforvalidclusterresult";
    private static final String DECAY_RATE_OPTION_NAME = "io.seldon.algorithm.clusters.decayratesecs";

    private static Logger logger = Logger.getLogger(BaseClusterCountsRecommender.class.getName());

    @Autowired
    JdoCountRecommenderUtils cUtils;
    
    public ItemRecommendationResultSet recommend(String recommenderName, String recommenderType, String client,
                                                 RecommendationContext ctxt, Long user, int dimensionId,
                                                 int maxRecsCount) {
        CountRecommender r = cUtils.getCountRecommender(client);
        RecommendationContext.OptionsHolder optionsHolder = ctxt.getOptsHolder();
        if (r != null)
        {
            long t1 = System.currentTimeMillis();
            Set<Long> exclusions = Collections.emptySet();
            if(ctxt.getMode()== RecommendationContext.MODE.EXCLUSION){
                exclusions = ctxt.getContextItems();
            }
            boolean includeShortTermClusters = recommenderType.equals("CLUSTER_COUNTS_DYNAMIC");
            Double longTermWeight = optionsHolder.getDoubleOption(LONG_TERM_WEIGHT_OPTION_NAME);
            Double shortTermWeight = optionsHolder.getDoubleOption(SHORT_TERM_WEIGHT_OPTION_NAME);
            Integer minClusterItems = optionsHolder.getIntegerOption(MIN_ITEMS_FOR_VALID_CLUSTER_OPTION_NAME);
            Double decayRate = optionsHolder.getDoubleOption(DECAY_RATE_OPTION_NAME);
            Map<Long, Double> recommendations = r.recommend(recommenderType, user, null, dimensionId, maxRecsCount, exclusions, includeShortTermClusters,
                    longTermWeight,shortTermWeight,decayRate,minClusterItems);
            long t2 = System.currentTimeMillis();
            logger.debug("Recommendation via cluster counts for user "+user+" took "+(t2-t1)+" and got back "+recommendations.size()+" recommednations");
            List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
            for (Map.Entry<Long, Double> entry : recommendations.entrySet()){
                results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
            }
            return new ItemRecommendationResultSet(results, recommenderName);
        } else {
            return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), recommenderName);
        }
    }
}
