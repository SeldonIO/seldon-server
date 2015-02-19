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

import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.UpdateRetriever;
import io.seldon.trust.impl.jdo.RecommendationUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ItemSimilarityRecommender {

	private static Logger logger = Logger.getLogger( ItemSimilarityRecommender.class.getName() );

	final static int RECOMMEND_CACHE_TIME_SECS = 3600;

	private String client;
	private IItemSimilarityPeer itemSimilarityPeer;

	public ItemSimilarityRecommender(String client,
									 IItemSimilarityPeer itemSimilarityPeer) {
		super();
		this.client = client;
		this.itemSimilarityPeer = itemSimilarityPeer;
	}


	public Map<Long,Double> recommend(long userId, int dimension, int numRecommendations, Set<Long> exclusions)
	{
		String memKey = MemCacheKeys.getItemRecommender(client, userId, dimension, -1);
		Map<Long,Double> res = (Map<Long,Double>) MemCachePeer.get(memKey);
		if (res == null)
		{
			res = itemSimilarityPeer.getRecommendations(userId, dimension, -1);
			if (res != null)
			{
				MemCachePeer.put(memKey, res, RECOMMEND_CACHE_TIME_SECS);
			}
			else
			{
				logger.info("No recommendation results for user "+userId+" dimension "+dimension+" for client "+client);
				return new HashMap<>();
			}
		}

		for(Long excl : exclusions)
			res.remove(excl);

		return RecommendationUtils.rescaleScoresToOne(res, numRecommendations);
	}
	public Map<Long,Double> recommendSimilarItems(long itemId, int dimension, int numRecommendations, Set<Long> exclusions)
	{
		return recommendSimilarItems(itemId, dimension, numRecommendations, exclusions, true);
	}

	public Map<Long,Double> recommendSimilarItems(final long itemId, final int dimension, int numRecommendations, Set<Long> exclusions,boolean rescaleScores)
	{
		String memKey = MemCacheKeys.getItemSimilarity(client, itemId, dimension, -1);
		Map<Long,Double> res = (Map<Long,Double>) MemCachePeer.get(memKey);
		Map<Long, Double> newRes = null;
		try{
			newRes = DogpileHandler.get().retrieveUpdateIfRequired(memKey, res, new UpdateRetriever<Map<Long, Double>>() {
				@Override
				public Map<Long, Double> retrieve() throws Exception {
					return itemSimilarityPeer.getSimilarItems(itemId, dimension, -1);
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

	public Map<Long,Double> recommendSimilarItems(List<Long> items, int dimension, int numRecommendations, Set<Long> exclusions)
	{
		Map<Long,Double> res = new HashMap<>();
		for(Long itemId : items)
		{
			logger.debug("Finding similar items to "+itemId);
			Map<Long,Double> scores = recommendSimilarItems(itemId, dimension, numRecommendations, exclusions,false);
			for(Map.Entry<Long,Double> score : scores.entrySet())
			{
				if (res.containsKey(score.getKey()))
					res.put(score.getKey(), score.getValue()+res.get(score.getKey()));
				else
					res.put(score.getKey(), score.getValue());
			}
		}

		logger.info("Found "+res.size()+" similar items based on history of "+items.size());
		return RecommendationUtils.rescaleScoresToOne(res, numRecommendations);
	}


}
