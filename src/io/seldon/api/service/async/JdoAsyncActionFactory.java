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

package io.seldon.api.service.async;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;


import org.apache.log4j.Logger;

public class JdoAsyncActionFactory {

	
	private static final String DEF_HOSTNAME = "TEST";
	private static Logger logger = Logger.getLogger(JdoAsyncActionFactory.class.getName());
	private static boolean active = false;
	
	
	private ConcurrentHashMap<String,AsyncActionQueue> queues = new ConcurrentHashMap<String,AsyncActionQueue>();
	private static JdoAsyncActionFactory factory;
	Timer updateCheckTimer;
	
	private static int DEF_QTIMEOUT_SECS = 5;
	private static int DEF_MAXQSIZE = 100000;
	private static int DEF_BATCH_SIZE = 2000;
	private static int DEF_DB_RETRIES = 3;
	private static boolean DEF_RUN_USERITEM_UPDATES = true;
	private static boolean DEF_UPDATE_IDS_ACTION_TABLE = true;
	private static boolean DEF_INSERT_ACTIONS = false;
	
	public static final String ASYNC_PROP_PREFIX = "io.seldon.asyncactions";
	private static Properties props;
	
	private static boolean asyncUserWrites = false;
	private static boolean asyncItemWrites = false;
	
	private static String localHostname;
	private static boolean updateIdsActionTable = true;
	
	private static String getHostname()
	{
		try
		{
			String EC2Id = "";
			String inputLine;
			URL EC2MetaData = new URL("http://169.254.169.254/latest/meta-data/local-hostname");
			HttpURLConnection EC2MD = (HttpURLConnection) EC2MetaData.openConnection();
			HttpURLConnection.setFollowRedirects(false);
			EC2MD.setConnectTimeout(5 * 1000);
			BufferedReader in = new BufferedReader(new InputStreamReader(EC2MD.getInputStream()));
			while ((inputLine = in.readLine()) != null)
			{
				EC2Id = inputLine;
			}
			in.close();
			logger.info("Local hostname is "+EC2Id);
			return EC2Id;
		}
		catch (IOException e)
		{
			logger.warn("Failed to get EC2 local hostname",e);
			return DEF_HOSTNAME;
		}
	}
	
	public static boolean isActive() {
		return active;
	}

	public static JdoAsyncActionFactory create(Properties properties)
	{
		localHostname = getHostname();
		props = properties;
		asyncUserWrites = "true".equals(props.getProperty(ASYNC_PROP_PREFIX+".users"));
		asyncItemWrites = "true".equals(props.getProperty(ASYNC_PROP_PREFIX+".items"));
		String isActive = props.getProperty(ASYNC_PROP_PREFIX+".active");
		active = "true".equals(isActive);
		if (active)
		{
			factory = new JdoAsyncActionFactory();
			String clientList = props.getProperty(ASYNC_PROP_PREFIX+".start");
			if (clientList != null && clientList.length() > 0)
			{
				String[] clients = clientList.split(",");
				for(int i=0;i<clients.length;i++)
				{
					factory.get(clients[i]);
				}
				
			}
			return factory;
		}
		else
			return null;
	}
	
	public static void shutdown()
	{
		if (factory != null)
		{
			factory.cancelTimer();
			factory.shutdownQueues();
		}
	}
	
	public static JdoAsyncActionFactory get()
	{
		return factory;
	}
	
	private static AsyncActionQueue create(String client)
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
		
		boolean runUserItemUpdates = DEF_RUN_USERITEM_UPDATES;
		val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".useritem.updates");
		if (val != null)
			runUserItemUpdates = "true".equals(val);
		
		boolean runUpdateIdsActionTable = DEF_UPDATE_IDS_ACTION_TABLE;
		val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".update.ids");
		if (val != null)
			runUpdateIdsActionTable = "true".equals(val);
		
		boolean insertActions = DEF_INSERT_ACTIONS;
		val = props.getProperty(ASYNC_PROP_PREFIX+"."+client+".insert.actions");
		if (val != null)
			insertActions = "true".equals(val);
		
		
		return create(client,qTimeout,batchSize,maxqSize,dbRetries,runUserItemUpdates,runUpdateIdsActionTable,insertActions);
	}
	
	private static AsyncActionQueue create(String client,int qtimeoutSecs,int batchSize,int qSize,int dbRetries,boolean runUserItemUpdates,boolean runUpdateIdsActionTable,boolean insertActions)
	{
		JdoAsyncActionQueue q = new JdoAsyncActionQueue(client,qtimeoutSecs,batchSize,qSize,dbRetries,runUserItemUpdates,runUpdateIdsActionTable,insertActions);
		Thread t = new Thread(q);
		t.start();
		return q;
	}
	
	public AsyncActionQueue get(String client)
	{
		AsyncActionQueue queue = queues.get(client);
		if (queue == null)
		{
			queues.putIfAbsent(client, create(client));
			queue = get(client);
		}
		return queue;
	}

	public static boolean isAsyncUserWrites() {
		return asyncUserWrites;
	}

	public static void setAsyncUserWrites(boolean asyncUserWrites) {
		JdoAsyncActionFactory.asyncUserWrites = asyncUserWrites;
	}

	public static boolean isAsyncItemWrites() {
		return asyncItemWrites;
	}

	public static void setAsyncItemWrites(boolean asyncItemWrites) {
		JdoAsyncActionFactory.asyncItemWrites = asyncItemWrites;
	}
	
	private void shutdownQueues()
	{
		for(Map.Entry<String,AsyncActionQueue> e : queues.entrySet())
		{
			e.getValue().setKeepRunning(false);
		}
	}
	
	private void cancelTimer()
	{
		if (updateCheckTimer != null)
			updateCheckTimer.cancel();
	}

	
	
}
