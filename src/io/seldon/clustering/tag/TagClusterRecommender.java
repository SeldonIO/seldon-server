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

package io.seldon.clustering.tag;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import org.apache.log4j.Logger;

import io.seldon.api.TestingUtils;
import io.seldon.trust.impl.jdo.RecommendationUtils;

public class TagClusterRecommender {

	private static Logger logger = Logger.getLogger(TagClusterRecommender.class.getName());
	
	static int maxItemCountsPerTag = 25;
	static final int TAG_CACHE_TIME_SECS = 600;
	boolean testMode = false;
	
	String client;
	ITagClusterCountStore store;
	IItemTagCache tagCache;
	int tagAttrId = 9;
	
	
	
	public TagClusterRecommender(String client, ITagClusterCountStore store,
			IItemTagCache tagCache, int tagAttrId) {
		super();
		this.client = client;
		this.store = store;
		this.tagCache = tagCache;
		this.tagAttrId = tagAttrId;
		this.testMode = TestingUtils.get().getTesting();
	}


	public Map<Long,Double> recommend(long userId,long itemId,Set<Long> userHistory,int dimension,int numRecommendations,Set<Long> exclusions,double decay)
	{
		Set<Long> items = new HashSet<Long>();
		items.add(itemId);
		if (userHistory != null)
			items.addAll(userHistory);
		Map<Long,Double> counts = null;
		for(Long itemUser : items)
		{
			Set<String> tags = tagCache.getTags(itemUser, tagAttrId);

			Map<Long,Double> countsTag = getCountsForTags(itemUser,tags, dimension, decay, exclusions);
			
			if (counts != null)
			{
				for(Map.Entry<Long, Double> e : countsTag.entrySet())
				{
					Double val = counts.get(e.getKey());
					if (val != null)
						counts.put(e.getKey(), val+e.getValue());
					else
						counts.put(e.getKey(), e.getValue());
				}
			}
			else
				counts = countsTag;
			
		}
		
		return RecommendationUtils.rescaleScoresToOne(counts, numRecommendations);
	}
	
	private  Map<Long,Double> getCountsForTags(long itemId,Set<String> tags,int dimension,double decay,Set<Long> exclusions)
	{
		String memcacheKey = MemCacheKeys.getTagsItemCounts(client, tags, dimension);
		Map<Long,Double> counts = (Map<Long,Double>) MemCachePeer.get(memcacheKey);
		if (counts == null)
		{
			logger.info("Getting form db counts for tags for item id "+itemId);
			counts = new HashMap<Long,Double>();
			for(String tag : tags)
			{
				Map<Long,Double> tagCounts = getCountsForTag(tag, maxItemCountsPerTag,dimension,decay);
				double maxCount = 0;
				for(Map.Entry<Long, Double> itemCount : tagCounts.entrySet())
					if (itemCount.getValue() > maxCount)
						maxCount = itemCount.getValue();
				if (maxCount > 0)
					for(Map.Entry<Long, Double> itemCount : tagCounts.entrySet())
					{
						Long item = itemCount.getKey();
						if (!exclusions.contains(item))
						{
							Double count = itemCount.getValue()/maxCount;
							Double existing = counts.get(item);
							if (existing != null)
								count = count + existing;
							counts.put(item, count);
						}
					}
			}
			MemCachePeer.put(memcacheKey, counts, TAG_CACHE_TIME_SECS);
		}
		else
			logger.info("Got from memcache counts for tags for item id "+itemId);
		return counts;
	}
	
	private Map<Long,Double> getCountsForTag(String tag,int maxCounts,int dimension,double decay)
	{
		String memcacheKey = MemCacheKeys.getTagItemCount(client, tag,dimension);
		Map<Long,Double> counts = (Map<Long,Double>) MemCachePeer.get(memcacheKey);
		if (counts == null)
		{
			counts = store.getTopCountsForDimension(tag, dimension, maxCounts,decay);
			if (!testMode)
				MemCachePeer.put(memcacheKey, counts, TAG_CACHE_TIME_SECS);
		}
		return counts;
	}

}
