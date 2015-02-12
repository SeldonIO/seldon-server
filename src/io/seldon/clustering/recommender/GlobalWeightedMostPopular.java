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

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class GlobalWeightedMostPopular {
	private static Logger logger = Logger.getLogger( GlobalWeightedMostPopular.class.getName() );
	private static GlobalWeightedMostPopular peer = null;
	
	private ConcurrentHashMap<String,MemoryWeightedClusterCountMap> countStoreMap;

	private static boolean active = false;
	private static int defDecay = 60*60*3;
	private static int defCacheSize = 1000;
	
	public static void initialise(Properties props)
	{
		peer = new GlobalWeightedMostPopular();
		String val = props.getProperty("io.seldon.weightedmostpopular.active");
		if (val != null)
			active = Boolean.valueOf(val);

		val = props.getProperty("io.seldon.weightedmostpopular.defdecay");
		if (val != null)
			defDecay = Integer.parseInt(val);
		
		val = props.getProperty("io.seldon.weightedmostpopular.defcachesize");
		if (val != null)
			defCacheSize = Integer.parseInt(val);
		
		logger.info("Global Weighted Most Popular initialised with active:"+active+" default decay (secs):"+defDecay+" default cache size:"+defCacheSize);
		
	}

	public static boolean isActive()
	{
		return active;
	}
	
	public static MemoryWeightedClusterCountMap get(String client)
	{
		MemoryWeightedClusterCountMap map =  peer.countStoreMap.get(client);
		if (map == null)
		{
			peer.countStoreMap.putIfAbsent(client, new MemoryWeightedClusterCountMap(defCacheSize, defDecay));
			return get(client);
		}
		else
			return map;
	}
	
	public GlobalWeightedMostPopular()
	{
		this.countStoreMap = new ConcurrentHashMap<String,MemoryWeightedClusterCountMap>();
	}
	

	
	
	
}
