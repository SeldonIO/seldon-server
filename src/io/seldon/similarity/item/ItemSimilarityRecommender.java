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

package io.seldon.similarity.item;

import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.UpdateRetriever;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.jdo.RecommendationUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

@Component
public class ItemSimilarityRecommender implements ItemRecommendationAlgorithm {

	private static final String RECENT_ACTIONS_PROPERTY_NAME = "io.seldon.algorithm.general.numrecentactionstouse";
	private static Logger logger = Logger.getLogger( ItemSimilarityRecommender.class.getName() );

	final static int RECOMMEND_CACHE_TIME_SECS = 3600;


	public Map<Long,Double> recommendSimilarItems(final String client, final long itemId, final int dimension, int numRecommendations, Set<Long> exclusions, boolean rescaleScores)
	{
		String memKey = MemCacheKeys.getItemSimilarity(client, itemId, dimension, -1);
		Map<Long,Double> res = (Map<Long,Double>) MemCachePeer.get(memKey);
		Map<Long, Double> newRes = null;
		try{
			newRes = DogpileHandler.get().retrieveUpdateIfRequired(memKey, res, new UpdateRetriever<Map<Long, Double>>() {
				@Override
				public Map<Long, Double> retrieve() throws Exception {
					IItemSimilarityPeer peer = new JdoItemSimilarityPeer(client);
					return peer.getSimilarItems(itemId, dimension, -1);
				}
			},RECOMMEND_CACHE_TIME_SECS);
		} catch (Exception e){
			logger.warn("Error when retrieving similar items in dogpile handler ", e);
		}
		if (newRes != null)
		{
			MemCachePeer.put(memKey, newRes, RECOMMEND_CACHE_TIME_SECS);
			res = newRes;
		}

		if(newRes==null && res == null)
		{
			logger.info("No similar item recommendation results for item "+itemId+" dimension "+dimension+" for client "+client);
			return new HashMap<>();
		}


		for(Long excl : exclusions)
			res.remove(excl);

		if (rescaleScores)
			return RecommendationUtils.rescaleScoresToOne(res, numRecommendations);
		else
			return res;
	}

	public List<ItemRecommendationResultSet.ItemRecommendationResult> recommendSimilarItems(
			String client, List<Long> items, int dimension, int numRecommendations, Set<Long> exclusions)
	{
		List<ItemRecommendationResultSet.ItemRecommendationResult> toReturn = new ArrayList<>();
		Map<Long,Double> res = new HashMap<>();
		for(Long itemId : items)
		{
			logger.debug("Finding similar items to "+itemId);
			Map<Long,Double> scores = recommendSimilarItems(client,itemId, dimension, numRecommendations, exclusions,false);
			for(Map.Entry<Long,Double> score : scores.entrySet())
			{
				if (res.containsKey(score.getKey()))
					res.put(score.getKey(), score.getValue()+res.get(score.getKey()));
				else
					res.put(score.getKey(), score.getValue());
			}
		}

		for (Map.Entry<Long, Double> entry : res.entrySet()) {
			toReturn.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(),entry.getValue().floatValue()));
		}

		logger.info("Found "+res.size()+" similar items based on history of "+items.size());
		return toReturn;
	}


	@Override
	public ItemRecommendationResultSet recommend(String client, Long user, int dimensionId, int maxRecsCount, RecommendationContext ctxt, List<Long> recentItemInteractions) {

		
		RecommendationContext.OptionsHolder opts = ctxt.getOptsHolder();
		int numRecentActionsToUse = opts.getIntegerOption(RECENT_ACTIONS_PROPERTY_NAME);
		List<Long> itemsToScore;
		if(recentItemInteractions.size() > numRecentActionsToUse)
		{
			logger.debug("Limiting recent items for score to size "+numRecentActionsToUse+" from present "+recentItemInteractions.size());
			itemsToScore = recentItemInteractions.subList(0, numRecentActionsToUse);
		}
		else
			itemsToScore = new ArrayList<>(recentItemInteractions);
		
		Set<Long> exclusions = ctxt.getContextItems();
		List<ItemRecommendationResultSet.ItemRecommendationResult> recommendations = null;
		if (itemsToScore.size() > 0) {
			recommendations = recommendSimilarItems(client, itemsToScore, dimensionId, maxRecsCount, exclusions);
			if (recommendations != null)
				logger.info("Recent similar items recommender returned " + recommendations.size() + " recommendations");
		} else
			logger.info("failing RECENT_SIMILAR_ITEMS as no recent actions");
		return new ItemRecommendationResultSet(recommendations);
	}

	@Override
	public String name() {
		return CFAlgorithm.CF_RECOMMENDER.RECENT_SIMILAR_ITEMS.name();
	}
}
