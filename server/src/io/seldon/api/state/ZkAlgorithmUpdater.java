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

package io.seldon.api.state;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.seldon.api.Util;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.curator.framework.CuratorFramework;

import io.seldon.recommendation.CFAlgorithm;

public class ZkAlgorithmUpdater implements Runnable {

	private static Logger logger = Logger.getLogger(ZkAlgorithmUpdater.class.getName());
	
	public static final String ALG_PATH = "/alg";
	private static final long maxSleepTime = 100000;
	String clientName;
	boolean keepRunning = true;
	long sleepTime = 1000;
	int numUpdates = 0;
	BlockingQueue<String> queue;
	ZkCuratorHandler curatorHandler;
	
	
	public ZkAlgorithmUpdater(String clientName, ZkCuratorHandler curatorHandler) {
		super();
		this.clientName = clientName;
		this.curatorHandler = curatorHandler;
	}

	public void clearUpdateCount()
	{
		this.numUpdates = 0;
	}

	public int getNumUpdates() {
		return numUpdates;
	}



	public void setKeepRunning(boolean keepRunning) {
		this.keepRunning = keepRunning;
	}

	
	public void addMessageToQueue(String message)
	{
		queue.add(message);
	}



	@Override
	public void run() 
	{
		logger.info("Starting");
		try
		{
			boolean error = false;
			while(keepRunning)
			{
				try
				{
					
       	 			CuratorFramework client = null;
       	 			boolean ok = false;
       	 			for(int attempts=0;attempts<4 && !ok;attempts++)
       	 			{
       	 				client = curatorHandler.getCurator().usingNamespace(clientName);
       	 				logger.info("Waiting until zookeeper connected on attempt "+attempts);
       	 				ok = client.getZookeeperClient().blockUntilConnectedOrTimedOut();
       	 				if (ok)
       	 					logger.info("zookeeper connected");
       	 				else
       	 				{
       	 					logger.error("Timed out waiting for zookeeper connect : attempt "+attempts);
       	 				}
       	 			}
       	 			if (!ok)
       	 			{
       	 				logger.error("Failed to connect to zookeeper after multiple attempts - STOPPING");
       	 				return;
       	 			}
       	 			queue = new LinkedBlockingQueue<>();
       	 			final Watcher watcher = new Watcher()
       	 			{
       	 				boolean expired = false;
       	 				
       	 				@Override
       	 				public void process(WatchedEvent event)
                    	{
       	 					try
       	 					{
       	 						if (event.getPath() != null)
       	 							queue.put(event.getPath());
       	 						else
       	 						{
       	 							logger.warn("Unexpected event "+event.getType().name()+" -> "+event.toString());
       	 							switch(event.getState())
       	 							{
       	 							case SyncConnected:
       	 							{
       	 							}
       	 							break;
       	 							case Expired:
       	 							{
	 									queue.put("");       	 									
       	 							}
       	 							break;
       	 							case Disconnected:
	 								{
	 									logger.warn("Disconnected from server");
	 									//queue.put("");       	 									
	 								}
	 								break;
       	 							}

       	 						}
       	 					}
       	 					catch ( InterruptedException e )
       	 					{
       	 						throw new Error(e);
       	 					}
                    	}
       	 			};
       	 			
       	 			
       	 			
       	 			logger.info("Checking path "+ALG_PATH+" exists");
       	 			if (client.checkExists().forPath(ALG_PATH) == null)
       	 			{
       	 				logger.warn("Path "+ALG_PATH+" does not exist for client "+clientName+" creating...");
       	 				client.create().forPath(ALG_PATH);
       	 			}
       	 			else
       	 				logger.info("Path "+ALG_PATH+" exists");
       	 			
       	 			//client.getConnectionStateListenable().addListener(stateListener);
       	 			boolean restart = false;
       	 			while(keepRunning && !restart)
       	 			{
       	 				
       	 				client.getData().usingWatcher(watcher).forPath(ALG_PATH);
    			
       	 				String path = queue.take();
       	 				if (!StringUtils.isEmpty(path))
       	 				{
       	 					logger.info("Alg Path changed "+path);
			
       	 					byte[] bytes = client.getData().forPath(ALG_PATH);

       	 					try
       	 					{
       	 						ObjectInputStream  in = new ObjectInputStream(new ByteArrayInputStream(bytes));
       	 						CFAlgorithm  alg = (CFAlgorithm) in.readObject();
       	 						in.close();
       	 						//Update Algorithm for client
       	 						logger.info("Updating algorithm options for "+clientName+" to "+alg.toString());
       	 						Util.getAlgorithmService().setAlgorithmOptions(clientName, alg);
       	 						numUpdates++;
       	 					}
       	 					catch (ClassNotFoundException e)
       	 					{
       	 						logger.error("Can't find class ",e);
       	 					}
       	 					catch (IOException e)
       	 					{
       	 						logger.error("Failed to deserialize algorithm for client "+clientName,e);
       	 					}
       	 				}
       	 				else
       	 				{
       	 					//logger.warn("Empty path - maybe zookeeper connection state change watcher will be reset");
       	 					logger.warn("Will try to restart");
       	 					restart = true;
       	 				}
       	 			}
       	 			
       	 			
				} 
				catch (IOException e) 
				{
					logger.error("Exception trying to create sk client ",e);
					error = true;
				} catch (Exception e) {
					logger.error("Exception from zookeeper client ",e);
					error = true;
				}
				finally
				{

				}
			
				if (keepRunning && error)
				{
					logger.info("Sleeping "+sleepTime);
					Thread.sleep(sleepTime);
					if (sleepTime * 2 < maxSleepTime)
						sleepTime = sleepTime * 2;
					error = false;
				}
				
			}
		} 
		catch (InterruptedException e) 
		{
			logger.warn("Sleep interuppted ",e);
		}
		logger.info("Stopping");
	}

}
