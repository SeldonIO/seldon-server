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

public class MemoryClusterCountFactory {

	private static Logger logger = Logger.getLogger(MemoryClusterCountFactory.class.getName());
	
	private static final String PROP_PREFIX = "io.seldon.memoryclusters.";
	public static final int DEF_CACHE_SIZE = 200; // default number of article to keep in each cluster
	public static final double DEF_THRESHOLD = 0; // min threshold for counts otherwise 0 is returned
	public static final double DEF_TIME_WEIGHTING_SECS = 600; // number of secs before item count no important
	public static final MemoryClusterCountStore.COUNTER_TYPE DEF_COUNTER_TYPE = MemoryClusterCountStore.COUNTER_TYPE.SIMPLE; // default type of memory counter
	
	private ConcurrentHashMap<String,MemoryClusterCountStore> stores = new ConcurrentHashMap<String,MemoryClusterCountStore>();
	private MemoryClusterCountStore.COUNTER_TYPE counterType = DEF_COUNTER_TYPE;

	private static MemoryClusterCountFactory factory;
	
	public static MemoryClusterCountFactory create(Properties props)
	{
		factory = new MemoryClusterCountFactory();
		String memoryCounterClients = props.getProperty("io.seldon.memoryclusters.clients");
		if (memoryCounterClients != null && memoryCounterClients.length() > 0)
		{
			String[] parts = memoryCounterClients.split(",");
			for(int i=0;i<parts.length;i++)
			{
				String client = parts[i];
				
				// Set cache size
				int cacheSize = DEF_CACHE_SIZE;
				String val = props.getProperty(PROP_PREFIX + client + ".cachesize");
				if (val != null)
					cacheSize = Integer.parseInt(val);
				
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
				
				MemoryClusterCountStore.COUNTER_TYPE counterType = DEF_COUNTER_TYPE;
				String type = props.getProperty(PROP_PREFIX + client + ".type");
				if (type != null)
				 counterType = MemoryClusterCountStore.COUNTER_TYPE.valueOf(type);
				
				factory.createStore(client, counterType, cacheSize, threshold, alpha);
			}
		}
		return factory;
	}
	
	public static MemoryClusterCountFactory get()
	{
		return factory;
	}
	
	private MemoryClusterCountFactory()
	{
		
	}
	
	private void createStore(String client,MemoryClusterCountStore.COUNTER_TYPE counterType,int cacheSize,double threshold,double alpha)
	{
		logger.info("Create store for "+client+" of type "+counterType.name()+" cache size "+cacheSize+" threshold "+threshold+" alpha "+alpha);
		stores.putIfAbsent(client, new MemoryClusterCountStore(counterType,cacheSize,threshold,alpha));
	}
	
	public MemoryClusterCountStore getStore(String client)
	{
		MemoryClusterCountStore store = stores.get(client);
		if (store == null)
		{
			return null;
			//createStore(client,counterType,DEF_CACHE_SIZE,DEF_THRESHOLD,DEF_TIME_WEIGHTING_SECS);
			//return getStore(client);
		}
		else
			return store;
		
	}
	
}
