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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.seldon.util.CollectionTools;

import edu.rit.pj.reduction.SharedDouble;

/**
 * A memory based counter for doubles based on long keys. Uses a caching concurrent map to store mappings with
 * a SharedDouble from JP library to atomically increment counters. SharedDouble uses CAS method over an
 * AtomicLong. Old entries will fall off end of map if infrequently used. Thus we (maybe) don't need to weight
 * the counts.
 * @author rummble
 *
 */
public class MemoryCachingClusterCountMap implements ClusterCounter {

	private ConcurrentMap<Long, SharedDouble> cache; 
	
	public MemoryCachingClusterCountMap(int cacheSize)
	{
		cache = new ConcurrentLinkedHashMap.Builder<Long, SharedDouble>()
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
		SharedDouble previous = cache.putIfAbsent(itemId, new SharedDouble(weight));
		if (previous != null)
			previous.addAndGet(weight);
	}

	/**
	 * Get the count from the map. Returning 0 if not there
	 * @param itemId
	 * @return count or 0 if not there
	 */
	@Override
	public double getCount(long itemId,long time)
	{
		SharedDouble previous = cache.get(itemId);
		if (previous != null)
			return previous.doubleValue();
		else
			return 0;
	}

	@Override
	public Map<Long, Double> getTopCounts(long time, int limit) {
		Map<Long,Double> map  = new HashMap<Long,Double>();
		for(Map.Entry<Long, SharedDouble> e : cache.entrySet())
			map.put(e.getKey(), e.getValue().get());
		return CollectionTools.sortMapAndLimit(map, limit);
	}
}
