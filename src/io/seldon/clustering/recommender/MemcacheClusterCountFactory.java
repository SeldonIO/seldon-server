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

public class MemcacheClusterCountFactory {
	private static Logger logger = Logger.getLogger(MemcacheClusterCountFactory.class.getName());

	public enum MEMCACHE_COUNTER_TYPE {SIMPLE,DECAY};
	
	private ConcurrentHashMap<String,ClusterCountStore> stores = new ConcurrentHashMap<String,ClusterCountStore>();

	private static final String PROP_PREFIX = "io.seldon.memcache.clusters.";
	public static final int DEF_CACHE_TIMEOUT_SECS = 60*60*24; // default number of article to keep in each cluster
	public static final double DEF_THRESHOLD = 0; // min threshold for counts otherwise 0 is returned
	public static final double DEF_TIME_WEIGHTING_SECS = 600; // number of secs before item count no important
	public static final MEMCACHE_COUNTER_TYPE DEF_COUNTER_TYPE = MEMCACHE_COUNTER_TYPE.SIMPLE; // default type of memory counter
	
	private MEMCACHE_COUNTER_TYPE counterType = DEF_COUNTER_TYPE;
	private static MemcacheClusterCountFactory factory;
	
	public static MemcacheClusterCountFactory create(Properties props)
	{
		factory = new MemcacheClusterCountFactory();
		String memcacheCounterClients = props.getProperty("io.seldon.memcache.clusters.clients");
		if (memcacheCounterClients != null && memcacheCounterClients.length() > 0)
		{
			String[] parts = memcacheCounterClients.split(",");
			for(int i=0;i<parts.length;i++)
			{
				String client = parts[i];
				
				// Set cache size
				int timeout = DEF_CACHE_TIMEOUT_SECS;
				String val = props.getProperty(PROP_PREFIX + client + ".timeout");
				if (val != null)
					timeout = Integer.parseInt(val);
				
				// set threshold
				double threshold = DEF_THRESHOLD;
				val = props.getProperty(PROP_PREFIX + client + ".threshold");
				if (val != null)
					threshold = Double.parseDouble(val);
				
				// set alpha exponential decay
				double alpha = DEF_TIME_WEIGHTING_SECS;
				val = props.getProperty(PROP_PREFIX + client + ".decay");
				if (val != null)
					alpha = Double.parseDouble(val);
				
				MEMCACHE_COUNTER_TYPE counterType = DEF_COUNTER_TYPE;
				String type = props.getProperty(PROP_PREFIX + client + ".type");
				if (type != null)
				 counterType = MEMCACHE_COUNTER_TYPE.valueOf(type);
				
				factory.createStore(client, counterType, timeout, threshold, alpha);
			}
		}
		return factory;
	}
	
	public static MemcacheClusterCountFactory get()
	{
		return factory;
	}
	
	private MemcacheClusterCountFactory()
	{
		
	}
	
	private void createStore(String client,MEMCACHE_COUNTER_TYPE counterType,int timeout,double threshold,double alpha)
	{
		logger.info("Create store for "+client+" of type "+counterType.name()+" timeout "+timeout+" threshold "+threshold+" alpha "+alpha);
		ClusterCountStore store;
		switch(counterType)
		{
		case DECAY:
			store = new MemcacheClusterCountDecayStore(client,timeout,threshold,alpha);
			break;
		case SIMPLE:
			store = new MemcacheClusterCountStore(client,timeout,threshold);
			break;
		default:
			store = new MemcacheClusterCountStore(client,timeout,threshold);
			break;

		}
		stores.putIfAbsent(client, store);
	}
	
	public ClusterCountStore getStore(String client)
	{
		ClusterCountStore store = stores.get(client);
		if (store == null)
		{
			return null;
			//createStore(client,counterType,DEF_CACHE_TIMEOUT_SECS,DEF_THRESHOLD,DEF_TIME_WEIGHTING_SECS);
			//return getStore(client);
		}
		else
			return store;
		
	}
}
