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

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class CooccurrencePeerFactory {

	private static Logger logger = Logger.getLogger(CooccurrencePeerFactory.class.getName());
	
	public static final long DEF_STALE_TIME_SECS = Long.MAX_VALUE;
	public static final int DEF_CACHE_SIZE = 10000;
	public static final String PROP_PREFIX = "io.seldon.cooc.";
	public static final String STALE_SECS_SUFFIX = ".stale.secs";
	public static final String CACHE_SIZE_SUFFIX = ".cache";

	private static ConcurrentHashMap<String,CooccurrencePeer> peers = new ConcurrentHashMap<>();
	
	public static void initialise(Properties props)
	{
		String clients = props.getProperty(PROP_PREFIX+"clients");
		if (clients != null && clients.length() > 0)
		{
			String[] parts = clients.split(",");
			for(int i=0;i<parts.length;i++)
			{
				String client = parts[i];
				
				// Stale time for cooc cache
				long staleTime = DEF_STALE_TIME_SECS;
				String val = props.getProperty(PROP_PREFIX + client + STALE_SECS_SUFFIX);
				if (val != null)
					staleTime = Long.parseLong(val);
				
				// Stale time for cooc cache
				int cacheSize = DEF_CACHE_SIZE;
				val = props.getProperty(PROP_PREFIX + client + CACHE_SIZE_SUFFIX);
				if (val != null)
					cacheSize = Integer.parseInt(val);
				
				logger.info("Creating CooccurrencePeer for client "+client+" with cacheSize "+cacheSize+" and stael time secs of "+staleTime);
				
				peers.put(client, new CooccurrencePeer(client, cacheSize, staleTime));
			}
		}
	}
	
	public static CooccurrencePeer get(String client)
	{
		if (peers.containsKey(client))
			return peers.get(client);
		else
		{
			logger.info("Creating CooccurrencePeer for client "+client+" with cacheSize "+DEF_CACHE_SIZE+" and stael time secs of "+DEF_STALE_TIME_SECS);
			peers.putIfAbsent(client, new CooccurrencePeer(client, DEF_CACHE_SIZE, DEF_STALE_TIME_SECS));
			return get(client);
		}
	}
	
}
