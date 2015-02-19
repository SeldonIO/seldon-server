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

package io.seldon.similarity.tagcount;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import io.seldon.api.TestingUtils;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.util.CollectionTools;

public class TagCountRecommender {

	private static Logger logger = Logger.getLogger( TagCountRecommender.class.getName() );

	private static final int EXPIRE_RECENT_ITEMS_SECS = 60; //Fixme - make part of algorithm or set larger
	
	private String client;
	private ITagCountPeer tagCountPeer;
	
	public TagCountRecommender(String client,
			ITagCountPeer tagCountPeer) {
		super();
		this.client = client;
		this.tagCountPeer = tagCountPeer;
	}

	public Map<Long,Double> recommend(long userId,int dimension,Set<Long> exclusions, int tagAttrId,int maxRecentItems,int numRecommendations)
	{
		Map<String,Integer> userTagMap = getUserTags(client, userId, 3600);
		if (userTagMap == null || userTagMap.size() == 0)
		{
			logger.debug("No tags found for user "+userId);
			return new HashMap<>();
		}
		else
		{
			Map<String,Set<Long>> itemTagMap = getRecentItems(client,maxRecentItems,tagAttrId,dimension,EXPIRE_RECENT_ITEMS_SECS);
			if (itemTagMap == null || itemTagMap.size() == 0)
			{
				logger.warn("Found no recent items for client "+client);
				return new HashMap<>();
			}
			else
			{
				Map<Long,Double> scores = new HashMap<>();
				double maxScore = 0;
				long maxItemId = 0;
				for(Map.Entry<String, Integer> utag : userTagMap.entrySet())
				{
					Set<Long> items = itemTagMap.get(utag.getKey());
					if (items != null)
						for(Long item : items)
						{
							Double c = scores.get(item);
							if (c == null)
								c = utag.getValue().doubleValue();
							else
								c = c + utag.getValue().doubleValue();
							if (c > maxScore)
								maxScore = c;
							if (item > maxItemId)
								maxItemId = item;
							scores.put(item, c);
						}
				}
				if (scores.size() >= 1)
				{
					maxItemId = maxItemId+1; // add one so for scoring below no increment is greater than one
					// add to all scores a delta increment to reflect time ordering using max item id
					for(Long item : scores.keySet())
					{
						scores.put(item, scores.get(item) +  maxItemId/item.doubleValue());
					}
					List<Long> topN = CollectionTools.sortMapAndLimitToList(scores, numRecommendations, true);
					Map<Long,Double> recMap = new HashMap<>();
					for(Long item : topN)
					{
						recMap.put(item, scores.get(item)/maxScore);
					}
					logger.info("Returning "+numRecommendations+" recs for user "+userId+" from "+scores.size()+" for client "+client);
					return recMap;
				}
				else
				{
					logger.info("Not enough recommendations for user "+userId+" for client "+client);
					return new HashMap<>();
				}
			}
		}
	}
	
	private Map<String,Integer> getUserTags(String client,long userId,int expireSecs)
	{
		String key = MemCacheKeys.getUserTags(client, userId);
		Map<String,Integer> tags = (Map<String,Integer>) MemCachePeer.get(key);
		if (tags == null)
		{
			tags = tagCountPeer.getUserTags(userId);
			MemCachePeer.put(key, tags, expireSecs);
		}
		return tags;
	}

	private Map<String,Set<Long>> getRecentItems(String client,int maxRecentItems,int tagAttrId,int dimension,int expireSecs)
	{
		String key = MemCacheKeys.getItemTags(client, maxRecentItems,dimension);
		Map<String,Set<Long>> recentItems = (Map<String,Set<Long>>) MemCachePeer.get(key);
		if (recentItems == null)
		{
			recentItems = tagCountPeer.getRecentItemTags(maxRecentItems, tagAttrId,dimension);
			if (!TestingUtils.get().getTesting())
				MemCachePeer.put(key, recentItems, expireSecs);
			else
				logger.warn("Not storing recent items as we seem to be in test mode");
		}
		return recentItems;
	}
	
	

}
