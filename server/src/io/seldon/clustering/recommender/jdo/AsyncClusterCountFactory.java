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

import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;
import io.seldon.api.state.options.DefaultOptions;
import io.seldon.db.jdo.DbConfigHandler;
import io.seldon.db.jdo.DbConfigListener;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AsyncClusterCountFactory implements DbConfigListener, ClientConfigUpdateListener {

	private static Logger logger = Logger.getLogger(AsyncClusterCountFactory.class.getName());

	private ConcurrentHashMap<String,AsyncClusterCountStore> queues = new ConcurrentHashMap<>();
	
	private static int DEF_QTIMEOUT_SECS = 5;
	private static int DEF_MAXQSIZE = 100000;
	private static int DEF_BATCH_SIZE = 4000;
	private static int DEF_DB_RETRIES = 3;
	private static int DEF_DECAY = 10800;
	private static boolean DEF_USE_DB_TIME = true;
	
	private static final String DECAY_RATE_KEY = "cluster_decay_rate";
	
	private static final String ASYNC_PROP_PREFIX = "io.seldon.async.counter";
	private ClientConfigHandler configHandler;
	
	DefaultOptions options;
	
	@Autowired
	public AsyncClusterCountFactory(DefaultOptions options, DbConfigHandler dbConfigHandler,ClientConfigHandler configHandler)
	{
		this.options = options;
		this.configHandler = configHandler;
		dbConfigHandler.addDbConfigListener(this);
	}

	 @PostConstruct
	 private void init(){
	        logger.info("Initializing...");
	        configHandler.addListener(this);
	    }
	
	public void clientDeleted(String client) {
		logger.info("Removing client:"+client);
		AsyncClusterCountStore q = queues.get(client);
		q.setKeepRunning(false);
		queues.remove(client);
	}

	private void createAndStore(String client)
	{
		queues.putIfAbsent(client, create(client));
	}
	
	private AsyncClusterCountStore create(String client)
	{
		int qTimeout = DEF_QTIMEOUT_SECS;
		int maxqSize = DEF_MAXQSIZE;
		int batchSize = DEF_BATCH_SIZE;
		int dbRetries = DEF_DB_RETRIES;
		double decay = DEF_DECAY;
		boolean useDBTime = DEF_USE_DB_TIME;
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

	@Override
	public void dbConfigInitialised(String client) {
		logger.info("Adding client:"+client);
		createAndStore(client);
	}

	@Override
	public void configUpdated(String client, String configKey, String configValue) {
		if (configKey.equals(DECAY_RATE_KEY))
		{
			logger.info("Received config updated for "+client+" with key "+configKey+" value "+configValue);
			if (queues.containsKey(client))
			{
				try
				{
					Double decayRate = Double.parseDouble(configValue);
					queues.get(client).setDecay(decayRate);
					logger.info("Updated decay rate for client "+client+" to "+decayRate);
				}
				catch (NumberFormatException e)
				{
					logger.error("Failed to parse decay rate for "+client+" value "+configValue);
				}
			}
		}

		
	}

	@Override
	public void configRemoved(String client, String configKey) {
		//DO NOTHING
	}
}
