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
package io.seldon.recommendation.baseline;

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.recommendation.RecommendationUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

@Component
public class RecentInteractionsRecommender  implements ItemRecommendationAlgorithm  {

	private static Logger logger = Logger.getLogger(RecentInteractionsRecommender.class.getSimpleName());

	private static final String name = RecentInteractionsRecommender.class.getSimpleName();

	
	@Override
	public ItemRecommendationResultSet recommend(String client, Long user,
			Set<Integer> dimensions, int maxRecsCount,
			RecommendationContext ctxt, List<Long> recentItemInteractions) {

		Map<Long,Double> scores = new HashMap<>();
		int maxScore = recentItemInteractions.size();
		double score = maxScore;
		for(Long item : recentItemInteractions)
		{
			scores.put(item, score);
			score = score - 1;
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
