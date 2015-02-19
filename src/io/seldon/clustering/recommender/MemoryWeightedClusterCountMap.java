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

/**
 * A memory based counter which provide an exponential decayed counter for each cluster id in a map.
 * @author rummble
 *
 */
public class MemoryWeightedClusterCountMap implements ClusterCounter {

	private ConcurrentMap<Long,ExponentialCount > cache; 
	double alpha;
	
	public MemoryWeightedClusterCountMap(int cacheSize,double alpha) {
		super();
		this.cache = new ConcurrentLinkedHashMap.Builder<Long, ExponentialCount>()
	    .maximumWeightedCapacity(cacheSize)
	    .build();
		this.alpha = alpha;
	}

	@Override
	public double getCount(long itemId,long time) {
		ExponentialCount c = cache.get(itemId);
		if (c != null)
			return c.get(time);
		else
			return 0;
	}

	@Override
	public void incrementCount(long itemId, double weight,long time) {
		ExponentialCount c = cache.putIfAbsent(itemId, new ExponentialCount(alpha,weight,time));
		if (c != null)
			c.increment(weight, time);
	}
	
	@Override
	public Map<Long, Double> getTopCounts(long time, int limit) {
		Map<Long,Double> map  = new HashMap<>();
		for(Map.Entry<Long, ExponentialCount> e : cache.entrySet())
			map.put(e.getKey(), e.getValue().get(time));
		return CollectionTools.sortMapAndLimit(map, limit);
	}

}
