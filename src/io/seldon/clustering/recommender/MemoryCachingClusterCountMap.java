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

import io.seldon.util.CollectionTools;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class MemoryCachingClusterCountMap implements ClusterCounter {

	private ConcurrentMap<Long, Double> cache; 
	
	public MemoryCachingClusterCountMap(int cacheSize)
	{
		cache = new ConcurrentLinkedHashMap.Builder<Long, Double>()
	    .maximumWeightedCapacity(cacheSize)
	    .build();
	}
	
	/**
	 * Try to put the weight into the cluster count map. If its already there increment weight 
	 * @param itemId
	 * @param weight
	 */
	@Override
	public void incrementCount(long itemId,double weight,long time)
	{
		//IGNORES TIME
		Double previous = cache.putIfAbsent(itemId, new Double(weight));
		if (previous != null)
			cache.put(itemId, previous+weight);
	}

	/**
	 * Get the count from the map. Returning 0 if not there
	 * @param itemId
	 * @return count or 0 if not there
	 */
	@Override
	public double getCount(long itemId,long time)
	{
		Double previous = cache.get(itemId);
		if (previous != null)
			return previous.doubleValue();
		else
			return 0;
	}

	@Override
	public Map<Long, Double> getTopCounts(long time, int limit) {
		Map<Long,Double> map  = new HashMap<Long,Double>();
		for(Map.Entry<Long, Double> e : cache.entrySet())
			map.put(e.getKey(), e.getValue());
		return CollectionTools.sortMapAndLimit(map, limit);
	}
}
