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

package io.seldon.clustering.tag;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class AsyncTagClusterCountFactory {
	private static Logger logger = Logger.getLogger(AsyncTagClusterCountFactory.class.getName());

	private ConcurrentHashMap<String,AsyncTagClusterCountStore> queues = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String,Boolean> activeClients = new ConcurrentHashMap<>();
	private static AsyncTagClusterCountFactory factory;
	
	private static int DEF_QTIMEOUT_SECS = 5;
	private static int DEF_MAXQSIZE = 100000;
	private static int DEF_BATCH_SIZE = 4000;
	private static int DEF_DB_RETRIES = 3;
	private static int DEF_DECAY = 43200;
	private static boolean DEF_USE_DB_TIME = true;
	
	private static final String ASYNC_PROP_PREFIX = "io.seldon.tag.async.counter";
	private static Properties props;
	
	
	
	
	public static AsyncTagClusterCountFactory create(Properties properties)
	{
		props = properties;
		String isActive = props.getProperty(ASYNC_PROP_PREFIX+".active");
		boolean active = "true".equals(isActive);
		factory = new AsyncTagClusterCountFactory();
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
	
	public static AsyncTagClusterCountFactory get()
	{
		return factory;
	}
	
	private void createAndStore(String client)
	{
		queues.putIfAbsent(client, create(client));
		activeClients.put(client, new Boolean(true));
	}
	
	private static AsyncTagClusterCountStore create(String client)
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
	
	
	
	private static AsyncTagClusterCountStore create(String client,int qtimeoutSecs,int batchSize,int qSize,int dbRetries,double decay,boolean useDBTime)
	{
		logger.info("Creating client for "+client+" qtimeout:"+qtimeoutSecs+" batchSize:"+batchSize+" qsize:"+qSize+" dbretries:"+dbRetries+" decay:"+decay+" useDBTime:"+useDBTime);
		AsyncTagClusterCountStore q = new AsyncTagClusterCountStore(client,qtimeoutSecs,batchSize,qSize,dbRetries,decay,useDBTime);
		Thread t = new Thread(q);
		t.start();
		return q;
	}
	
	public AsyncTagClusterCountStore get(String client)
	{
		Boolean isActive = activeClients.get(client);
		if (isActive != null && isActive)
		{
			AsyncTagClusterCountStore queue = queues.get(client);
			if (queue == null)
			{
				//queues.putIfAbsent(client, create(client));
				//queue = get(client);
			}
			return queue;
		}
		else
			return null;
	}
	
	public void setActive(String client,boolean val)
	{
		logger.info("Updating client "+client+" to active:"+val);
		// update if TRUE and we have settings for this client OR we already have an activity record for the client
		if ((val && queues.get(client) != null) || (!val && activeClients.containsKey(client)))
			activeClients.put(client, val);
	}
	
	

}
