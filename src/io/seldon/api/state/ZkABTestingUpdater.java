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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.apache.curator.framework.CuratorFramework;
import io.seldon.api.resource.DynamicParameterBean;
import io.seldon.api.service.ABTestingServer;
import io.seldon.api.service.ABTest;
import io.seldon.api.service.DynamicParameterServer;

public class ZkABTestingUpdater  implements Runnable {

	private static Logger logger = Logger.getLogger(ZkABTestingUpdater.class.getName());
	
	public static final String DYNAMIC_PARAM_PATH = "/dparam";
	public static final String AB_ALG_PATH = "/abalg";
	private static final long maxSleepTime = 100000;
	String clientName;
	boolean keepRunning = true;
	long sleepTime = 1000;
	int numUpdates = 0;
	BlockingQueue<String> queue;
	ZkCuratorHandler curatorHandler;
	
	public ZkABTestingUpdater(String clientName,ZkCuratorHandler curatorHandler) {
		super();
		this.clientName = clientName;
		this.curatorHandler = curatorHandler;
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
			while(keepRunning)
			{
				boolean error = false;
				try
				{
					
       	 			CuratorFramework client = null;
       	 			boolean ok = false;
       	 			for(int attempts=0;attempts<4 && !ok;attempts++)
       	 			{
       	 				client = curatorHandler.getCurator().usingNamespace(clientName);
       	 				logger.info("Waiting until zookeeper connected");
       	 				ok = client.getZookeeperClient().blockUntilConnectedOrTimedOut();
       	 				if (ok)
       	 					logger.info("zookeeper connected on attempt "+attempts);
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
       	 			Watcher watcher = new Watcher()
       	 			{
       	 				boolean expired;
       	 				
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
       	 			
       	 			logger.info("Checking path "+DYNAMIC_PARAM_PATH+" exists");
       	 			if (client.checkExists().forPath(DYNAMIC_PARAM_PATH) == null)
       	 			{
       	 				logger.warn("Path "+DYNAMIC_PARAM_PATH+" does not exist for client "+clientName+" creating...");
       	 				client.create().forPath(DYNAMIC_PARAM_PATH);
       	 			}

       	 			logger.info("Checking path "+AB_ALG_PATH+" exists");
       	 			if (client.checkExists().forPath(AB_ALG_PATH) == null)
       	 			{
       	 				logger.warn("Path "+AB_ALG_PATH+" does not exist for client "+clientName+" creating...");
       	 				client.create().forPath(AB_ALG_PATH);
       	 			}
       	 			
       	 			client.getData().usingWatcher(watcher).forPath(DYNAMIC_PARAM_PATH);
       	 			client.getData().usingWatcher(watcher).forPath(AB_ALG_PATH);
		
       	 			boolean restart = false;
       	 			while(keepRunning && !restart)
       	 			{
       	 				
       	 				String path = queue.take();
       	 				if (!StringUtils.isEmpty(path))
       	 				{
       	 					logger.info("Alg Path changed "+path);
   	 				
       	 					System.out.println("Alg Path changed "+path);
       	 				
       	 					if (path.endsWith(DYNAMIC_PARAM_PATH))
       	 					{
       	 						try
       	 						{
       	 							byte[] bytes = client.getData().forPath(DYNAMIC_PARAM_PATH);
           	 						ObjectInputStream  in = new ObjectInputStream(new ByteArrayInputStream(bytes));
           	 						DynamicParameterBean  bean = (DynamicParameterBean) in.readObject();
           	 						in.close();
           	 						logger.info("Updating dynamic parameter: "+bean.getName()+" and setting to value: "+bean.getValue()+" for client "+clientName);
           	 						DynamicParameterServer.setParameterBean(clientName, bean);
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
       	 						finally
       	 						{
       	 							client.getData().usingWatcher(watcher).forPath(DYNAMIC_PARAM_PATH);
       	 						}
   	 						
       	 					}
       	 					else if (path.endsWith(AB_ALG_PATH))
       	 					{
       	 						try
       	 						{
       	 							byte[] bytes = client.getData().forPath(AB_ALG_PATH);
       	 					
       	 							ObjectInputStream  in = new ObjectInputStream(new ByteArrayInputStream(bytes));
       	 							ABTest abTest = (ABTest) in.readObject();
       	 							in.close();
       	 							logger.info("Updating Algorithm percentage for client "+clientName+" to "+abTest.toString());
       	 							ABTestingServer.setABTest(clientName,abTest);
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
	       	 					finally
	       	 					{
	       	 						client.getData().usingWatcher(watcher).forPath(AB_ALG_PATH);
	       	 					}
       	 					}
       	 					else
       	 					{
       	 						logger.error("Unknown path "+path+" changed so reseting watchers");
       	 					}
       	 				}
       	 				else
       	 				{
       	 					//client.getData().usingWatcher(watcher).forPath(DYNAMIC_PARAM_PATH);
	 						//client.getData().usingWatcher(watcher).forPath(AB_ALG_PATH);
       	 					logger.warn("Will try to restart");
       	 					restart = true;
       	 				}
       	 				
       	 			}
       	 			
       	 			
				} 
				catch (IOException e) 
				{
					logger.error("Exception trying to create sk client ",e);
					error=true;
				} catch (Exception e) {
					logger.error("Exception from zookeeper client ",e);
					error=true;
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
