/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.tags;

import io.seldon.clustering.recommender.BaseClusterCountsRecommender;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.ItemRecommendationResultSet.ItemRecommendationResult;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.tags.UserTagAffinityManager.UserTagStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserTagAffinityRecommender extends BaseClusterCountsRecommender implements ItemRecommendationAlgorithm {
	private static Logger logger = Logger.getLogger(UserTagAffinityRecommender.class.getName());
	private static final String name = UserTagAffinityRecommender.class.getSimpleName();
	private static final String MIN_ITEMS_FOR_VALID_CLUSTER_OPTION_NAME
       = "io.seldon.algorithm.clusters.minnumberitemsforvalidclusterresult";
	private static final String DECAY_RATE_OPTION_NAME = "io.seldon.algorithm.clusters.decayratesecs";
	private static final String TAG_ATTR_ID_OPTION_NAME = "io.seldon.algorithm.tags.attrid";
	
	UserTagAffinityManager tagAffinityManager;
    JdoCountRecommenderUtils cUtils;
	
	@Autowired
	public UserTagAffinityRecommender(UserTagAffinityManager tagAffinityManager,JdoCountRecommenderUtils cUtils) {
		this.tagAffinityManager= tagAffinityManager;
		this.cUtils = cUtils;
	}
	
    @Override
    public ItemRecommendationResultSet recommend(String client, Long user, int dimensionId, int maxRecsCount,
                                                 RecommendationContext ctxt, List<Long> recentItemInteractions) {
        UserTagStore tagStore = tagAffinityManager.getStore(client);
        if (tagStore == null)
        {
        	logger.debug("Failed to get tag store for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
        }
        Map<String,Float> tagMap = tagStore.userTagAffinities.get(user);
        if (tagMap == null)
        {
        	logger.debug("Failed to get tag map for user "+user);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);        	
        }
        
        RecommendationContext.OptionsHolder optionsHolder = ctxt.getOptsHolder();
        Set<Long> exclusions = Collections.emptySet();
        if (ctxt.getMode() == RecommendationContext.MODE.EXCLUSION) {
        	exclusions = ctxt.getContextItems();
        }
        CountRecommender r = cUtils.getCountRecommender(client);
        if (r != null) {
        	long t1 = System.currentTimeMillis();
        	Integer minClusterItems = optionsHolder.getIntegerOption(MIN_ITEMS_FOR_VALID_CLUSTER_OPTION_NAME);
        	Double decayRate = optionsHolder.getDoubleOption(DECAY_RATE_OPTION_NAME);
        	Integer tagAttrId = optionsHolder.getIntegerOption(TAG_ATTR_ID_OPTION_NAME);
        	Map<Long, Double> recommendations = r.recommendUsingTag(tagMap, tagAttrId, dimensionId, maxRecsCount, exclusions, decayRate, minClusterItems);
                long t2 = System.currentTimeMillis();
                logger.debug("Recommendation via cluster counts for item  " + ctxt.getCurrentItem() + " for user " + user + " took " + (t2 - t1));
                List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
                for (Map.Entry<Long, Double> entry : recommendations.entrySet()) {
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
