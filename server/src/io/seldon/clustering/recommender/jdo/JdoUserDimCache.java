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

package io.seldon.clustering.recommender.jdo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

public class JdoUserDimCache {

	private static Logger logger = Logger.getLogger(JdoUserDimCache.class.getName());
	
	private static JdoUserDimCache cache = new JdoUserDimCache();;
	
	public static void initialise(Properties props)
	{
		cache = new JdoUserDimCache();
		String clientsProp = props.getProperty("io.seldon.facebook.categorytodim.clients");
		if (clientsProp != null)
		{
			String[] clients = clientsProp.split(",");
			for(int i=0;i<clients.length;i++)
			{
				String client = clients[i];
				logger.info("Adding to cache for client "+client);
				cache.addCacheEntry(client);
			}
		}
	}
	
	public static JdoUserDimCache get()
	{
		return cache;
	}
	
	
	
	public static class CacheEntry
	{
		Map<String,Set<Integer>> entry;

		public CacheEntry(Map<String, Set<Integer>> entry) {
			super();
			this.entry = entry;
		}
		
		
	}
	
	ConcurrentMap<String,CacheEntry> entries = new ConcurrentHashMap<>();
	
	public void addCacheEntry(String client)
	{
		FBToUserDimStore store = new FBToUserDimStore(client);
		CacheEntry e = new CacheEntry(store.getMappings());
		logger.info("Adding for client "+client+" "+e.entry.size()+" mappings ");
		entries.put(client, e);
	}

	public Map<String,Set<Integer>> getEntry(String client)
	{
		Map<String,Set<Integer>> r;
		CacheEntry c = entries.get(client);
		if (c == null)
			r = new HashMap<>();
		else
			r = c.entry;
		return r;
	}
}
