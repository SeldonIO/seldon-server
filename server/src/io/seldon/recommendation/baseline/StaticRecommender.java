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
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.ExceptionSwallowingMemcachedClient;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.UpdateRetriever;
import io.seldon.recommendation.RecommendationUtils;
import io.seldon.recommendation.baseline.jdo.SqlStaticRecommendationsProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StaticRecommender  implements ItemRecommendationAlgorithm  {

	private static Logger logger = Logger.getLogger(StaticRecommender.class.getSimpleName());

	private static final String name = StaticRecommender.class.getSimpleName();
	final static int RECOMMEND_CACHE_TIME_SECS = 3600;
	
	final ExceptionSwallowingMemcachedClient memcacheClient;
	
	
	@Autowired
	public StaticRecommender(ExceptionSwallowingMemcachedClient memcacheClient)
	{
		this.memcacheClient = memcacheClient;
	}
	
	@Override
	public ItemRecommendationResultSet recommend(final String client, final Long user,final Set<Integer> dimensions,final int maxRecsCount,RecommendationContext ctxt, List<Long> recentItemInteractions) {

		String memKey = MemCacheKeys.getStaticRecommendationsKey(client, user, dimensions, maxRecsCount);
		Map<Long,Double> scores = (Map<Long,Double>) memcacheClient.get(memKey);
		Map<Long, Double> newRes = null;
		try{
			newRes = DogpileHandler.get().retrieveUpdateIfRequired(memKey, scores, new UpdateRetriever<Map<Long, Double>>() {
				@Override
				public Map<Long, Double> retrieve() throws Exception {
					IStaticRecommendationsProvider peer = new SqlStaticRecommendationsProvider(client);
					return peer.getStaticRecommendations(user, dimensions, maxRecsCount);
				}
			},RECOMMEND_CACHE_TIME_SECS);
		} catch (Exception e){
			logger.warn("Error when retrieving static recommendations in dogpile handler ", e);
		}
		if (newRes != null)
		{
			memcacheClient.set(memKey, RECOMMEND_CACHE_TIME_SECS, newRes);
			scores = newRes;
		}

		if (scores == null)
		{
			return new ItemRecommendationResultSet(Collections.<ItemRecommendationResultSet.ItemRecommendationResult>emptyList(), name);
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

