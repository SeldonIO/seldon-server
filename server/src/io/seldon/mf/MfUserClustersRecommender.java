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
package io.seldon.mf;

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.recommendation.Recommendation;
import io.seldon.recommendation.RecommendationUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MfUserClustersRecommender  implements ItemRecommendationAlgorithm {
	private static Logger logger = Logger.getLogger(MfUserClustersRecommender.class.getSimpleName());

	private static final String name = MfUserClustersRecommender.class.getSimpleName();
	private MfUserClustersModelManager modelManager;
	
	@Autowired
	public MfUserClustersRecommender(MfUserClustersModelManager modelManager) {
		this.modelManager = modelManager;
	}
	
	/**
	 * Note this recommender does not respect any dimensions passed in
	 */
	@Override
	public ItemRecommendationResultSet recommend(String client, Long user,
			Set<Integer> dimensions, int maxRecsCount,
			RecommendationContext ctxt, List<Long> recentItemInteractions) {
		RecommendationContext.OptionsHolder options = ctxt.getOptsHolder();
		MfUserClustersModelManager.MfUserModel model = modelManager.getClientStore(client, options);
		if (model == null)
		{
			if (logger.isDebugEnabled())
				logger.debug("Failed to find ms cluster model for client "+client);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
		}
		
		Integer clusterIdx = model.userClusters.get(user);
		if (clusterIdx == null)
		{
			if (logger.isDebugEnabled())
				logger.debug("No user cluster for user "+user);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
		}
		
		List<Recommendation> recsAll = model.recommendations.get(clusterIdx);
		if (recsAll == null)
		{
			logger.error("Failed to find recommendations for cluster id "+clusterIdx);
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
		}
		
		Map<Long,Double> scores = new HashMap<>();
		Set<Long> exclusions = Collections.emptySet();
        if (ctxt.getMode() == RecommendationContext.MODE.EXCLUSION) {
        	exclusions = ctxt.getContextItems();
        }
		for(Recommendation candidate : recsAll)
		{
			if (!exclusions.contains(candidate.getContent()))
			{
				scores.put(candidate.getContent(), candidate.getPrediction());
				if (scores.size() >= maxRecsCount)
					break;
			}
		}
		Map<Long,Double> scaledScores = RecommendationUtils.rescaleScoresToOne(scores, maxRecsCount);
		List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
		for(Map.Entry<Long, Double> e : scaledScores.entrySet())
		{
			results.add(new ItemRecommendationResultSet.ItemRecommendationResult(e.getKey(), e.getValue().floatValue()));
		}
		return new ItemRecommendationResultSet(results, name);
	}

	@Override
	public String name() {
		return name;
	}

}
