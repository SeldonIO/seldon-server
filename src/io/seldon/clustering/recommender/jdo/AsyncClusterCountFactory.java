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

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncClusterCountFactory {
private static boolean active = false;
	
	private ConcurrentHashMap<String,AsyncClusterCountStore> queues = new ConcurrentHashMap<String,AsyncClusterCountStore>();
	private static AsyncClusterCountFactory factory;
	
	private static int DEF_QTIMEOUT_SECS = 5;
	private static int DEF_MAXQSIZE = 100000;
	private static int DEF_BATCH_SIZE = 4000;
	private static int DEF_DB_RETRIES = 3;
	private static int DEF_DECAY = 43200;
	private static boolean DEF_USE_DB_TIME = true;
	
	private static final String ASYNC_PROP_PREFIX = "io.seldon.async.counter";
	private static Properties props;
	
	
	public static boolean isActive() {
		return active;
	}

	public static AsyncClusterCountFactory create(Properties properties)
	{
		props = properties;
		String isActive = props.getProperty(ASYNC_PROP_PREFIX+".active");
		active = "true".equals(isActive);
		factory = new AsyncClusterCountFactory();
		if (active)
		{
			String clientList = props.getProperty(ASYNC_PROP_PREFIX+".start");
			if (clientList != null && clientList.length() > 0)
			{
				String[] clients = clientList.split(",");
				for(int i=0;i<clients.length;i++)
				{
					factory.createAndStore(clients[i]);
				}
				
			}
			return factory;
		}
		else
			return null;
	}
	
	public static AsyncClusterCountFactory get()
	{
		return factory;
	}
	
	private void createAndStore(String client)
	{
		queues.putIfAbsent(client, create(client));
	}
	
	private static AsyncClusterCountStore create(String client)
	{
		int qTimeout = DEF_QTIMEOUT_SECS;
		String val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".qtimeout");
		if (val != null)
			qTimeout = Integer.parseInt(val);
		
		int maxqSize = DEF_MAXQSIZE;
		val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".maxqsize");
		if (val != null)
			maxqSize = Integer.parseInt(val);

		int batchSize = DEF_BATCH_SIZE;
		val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".batchsize");
		if (val != null)
			batchSize = Integer.parseInt(val);

		int dbRetries = DEF_DB_RETRIES;
		val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".dbretries");
		if (val != null)
			dbRetries = Integer.parseInt(val);
		
		double decay = DEF_DECAY;
		val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".decay");
		if (val != null)
			decay = Double.parseDouble(val);
		 
		boolean useDBTime = DEF_USE_DB_TIME;
		val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".useDBTime");
		if (val != null)
			useDBTime = Boolean.parseBoolean(val);
		
		
		return create(client,qTimeout,batchSize,maxqSize,dbRetries,decay,useDBTime);
	}
	
	
	
	private static AsyncClusterCountStore create(String client,int qtimeoutSecs,int batchSize,int qSize,int dbRetries,double decay,boolean useDBTime)
	{
		AsyncClusterCountStore q = new AsyncClusterCountStore(client,qtimeoutSecs,batchSize,qSize,dbRetries,decay,useDBTime);
		Thread t = new Thread(q);
		t.start();
		return q;
	}
	
	public AsyncClusterCountStore get(String client)
	{
		AsyncClusterCountStore queue = queues.get(client);
		if (queue == null)
		{
			//queues.putIfAbsent(client, create(client));
			//queue = get(client);
		}
		return queue;
	}
	
}
