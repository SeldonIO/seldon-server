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

package io.seldon.cooc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.seldon.api.TestingUtils;

public class CooccurrencePeer {
	private static Logger logger = Logger.getLogger( CooccurrencePeer.class.getName() );
	
	ConcurrentLinkedHashMap<String,CooccurrenceCount> cache;
	long staleCountTimeSecs = Long.MAX_VALUE;
	String client;

	public CooccurrencePeer(String client,int cacheSize,long staleTime)
	{
		this.client = client;
		cache = new ConcurrentLinkedHashMap.Builder<String,CooccurrenceCount>()
			    .maximumWeightedCapacity(cacheSize)
			    .build();
		this.staleCountTimeSecs = staleTime;
		logger.info("Creating CooccurrencePeer for client "+client+" with cache size "+cacheSize+" and stale time of "+staleCountTimeSecs);
	}
	
	public long getCacheSize()
	{
		return cache.capacity();
	}
	
	
	
	public long getStaleCountTimeSecs() {
		return staleCountTimeSecs;
	}

	public String getClient() {
		return client;
	}

	public static String getKey(long item1,long item2)
	{
		if (item1 <= item2)
			return new StringBuffer(30).append(item1).append(":").append(item2).toString();
		else
			return new StringBuffer(30).append(item2).append(":").append(item1).toString();
	}
	
	public static long[] getKeyItems(String key)
	{
		String parts[] = key.split(":");
		return new long[] {Long.parseLong(parts[0]),Long.parseLong(parts[1])};
	}
	
	/**
	 * Returns counts for self join for list (a1,a2..aN) returns counts for a1:a1,a2:a2,a3:a3...aN:aN
	 * @param items1
	 * @param coocStore
	 * @return
	 */
	public Map<String,CooccurrenceCount> getCountsAA(List<Long> items1,ICooccurrenceStore coocStore)
	{
		Map<String,CooccurrenceCount> map = new HashMap<>();
		//get what we can from cache and modify lists to whats left (remove from cache stale items)
		List<Long> items1DB = new ArrayList<>();
		long now = TestingUtils.getTime();
		for(Long i1 : items1)
		{
			String key = getKey(i1,i1);
			CooccurrenceCount c = cache.get(key);
			if (c != null)
			{
				if (now - c.getTime() > this.staleCountTimeSecs) // remove stale item
				{
					cache.remove(key);
					c = null;
				}
			}
			if (c == null) // add to items2 to find in db and set item1 flag so its kept
			{
				items1DB.add(i1);
				map.put(key, new CooccurrenceCount(0,0)); //place zero result in case store does not return anything
			}
			else
				map.put(key, c); // place in result
		}
		
		//get from store whats not in cache (update cache from result)
		if (items1DB.size() > 0)
		{
			Map<String,CooccurrenceCount> counts = coocStore.getCounts(items1DB);
			map.putAll(counts);
			cache.putAll(counts);
		}
		
		return map;
	}
	
	/**
	 * Returns counts for cartesian product (join) from 2 lists (a1,a2..aN) (b1,b2..bN) - a1:b2, a1:b2...a1:bN,a2:b1,a2:b2,... etc
	 * @param items1 - list of item keys in outer loop to find
	 * @param items2 - list of item keys in inner loop to find
	 * @param coocStore
	 * @return
	 */
	public Map<String,CooccurrenceCount> getCountsAB(List<Long> items1,List<Long> items2,ICooccurrenceStore coocStore)
	{
		Map<String,CooccurrenceCount> map = new HashMap<>();
		
		//get what we can from cache and modify lists to whats left (remove from cache stale items)
		List<Long> items1DB = new ArrayList<>();
		List<Long> items2DB = new ArrayList<>();
		Set<Long> items2ForDB = new HashSet<>();
		long now = TestingUtils.getTime();
		for(Long i1 : items1)
		{
			boolean keepItem1 = false;
			for(Long i2 : items2)
			{
				String key = getKey(i1,i2);
				CooccurrenceCount c = cache.get(key);
				if (c != null)
				{
					if (now - c.getTime() > this.staleCountTimeSecs) // remove stale item
					{
						cache.remove(key);
						c = null;
					}
				}
				if (c == null) // add to items2 to find in db and set item1 flag so its kept
				{
					items2ForDB.add(i2);
					map.put(key, new CooccurrenceCount(0,0)); //place zero result in case store does not return anything
					keepItem1 = true;
				}
				else
					map.put(key, c); // place in result
			}
			if (keepItem1)
				items1DB.add(i1);
		}
		items2DB.addAll(items2ForDB);
		
		//get from store whats not in cache (update cache from result)
		if (items1DB.size() > 0)
		{
			Map<String,CooccurrenceCount> counts = coocStore.getCounts(items1DB, items2DB);
			map.putAll(counts);
			cache.putAll(counts);
		}
		return map;
	}
}
