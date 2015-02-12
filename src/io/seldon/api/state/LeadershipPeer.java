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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;

public class LeadershipPeer  implements Closeable, LeaderSelectorListener, Runnable
{
	private static Logger logger = Logger.getLogger(LeadershipPeer.class.getName());

	public static final String LEADER_PATH = "/apileader";
	private boolean keepRunning = true;
	private static final long maxSleepTime = 100000;
    //private final String name;
    private LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();
    private String server;
    private volatile Thread ourThread = null;
    private String serviceName;
    long sleepTime = 1000;
    boolean needsReconnect = false;
    boolean isLeader = false;

    public LeadershipPeer(String server,String serviceName)
    {
       this.server = server;
       this.serviceName = serviceName;
    }

    
    
    public boolean isLeader() {
		return isLeader;
	}



	public boolean isKeepRunning() {
		return keepRunning;
	}



	public void setKeepRunning(boolean keepRunning) {
		this.keepRunning = keepRunning;
	}



	public void start() throws IOException
    {
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
		logger.info("Start called");
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException
    {
    	logger.info("close called");
        leaderSelector.close();
        
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception
    {
        // we are now the leader. This method should not return until we want to relinquish leadership

        final int waitSeconds = (int)(5 * Math.random()) + 1;

        ourThread = Thread.currentThread();
        String name = ourThread.getName();
        logger.info(name + " is now the leader. Waiting " + waitSeconds + " seconds...");
        logger.info(name + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
        isLeader = true;
        try
        {
        	while(true)
        	{
        		Thread.sleep(5000);
        	}
        }
        catch ( InterruptedException e )
        {
            logger.info(name + " was interrupted.");
            Thread.currentThread().interrupt();
        }
        finally
        {
            ourThread = null;
            logger.info(name + " relinquishing leadership.\n");
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        // you MUST handle connection state changes. This WILL happen in production code.

        if ( (newState == ConnectionState.LOST) || (newState == ConnectionState.SUSPENDED) )
        {
            if ( ourThread != null )
            {
            	isLeader = false;
                ourThread.interrupt();
            }
        }
    }
    
 
	@Override
	public void run() {
		logger.info("Starting");
		try
		{
			
			while(keepRunning)
			{
				boolean error = false;
				needsReconnect = false;
				
				
					CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
					logger.info("Trying to connect to servers at "+server);
					CuratorFramework client = builder.connectString(server).namespace(serviceName).retryPolicy(new ExponentialBackoffRetry(1000,100)).build();
					try
					{
						client.start();
       	 				
						ConnectionStateListener stateListener = new ConnectionStateListener() {
						
						@Override
						public void stateChanged(CuratorFramework client, ConnectionState newState) {
							switch (newState)
							{
							case LOST:
								logger.warn("Lost connectiontion to Zookeeper will set needsReconnect flag");
								needsReconnect = true;
								break;
								
							case CONNECTED:
							case SUSPENDED:
							case RECONNECTED:
								break;
							}
							
							}
						};
       	 			
						client.getConnectionStateListenable().addListener(stateListener);
						// create a leader selector using the given path for management
						// all participants in a given leader selection must use the same path
						// ExampleClient here is also a LeaderSelectorListener but this isn't required
						leaderSelector = new LeaderSelector(client, LEADER_PATH, this);

						// for most cases you will want your instance to requeue when it relinquishes leadership
						leaderSelector.autoRequeue();
       	 				
						start();
					
						while(!needsReconnect && keepRunning)
						{
							Thread.sleep(5000);
						}
					
				
       	 				
					}
					catch (InterruptedException e)
					{
						logger.info("Was interuppted ");
					}
					catch (Exception e) 
					{
						logger.error("Exception from zookeeper client ",e);
						error = true;
					}
					finally
					{
						try
						{
							isLeader = false;
							if (ourThread != null)
								ourThread.interrupt();
					
							leaderSelector.close();

							Thread.sleep(1000);
						
							client.close();
					
						}
						catch (Exception e)
						{
							logger.error("Error while cleaning things up",e);
						}
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
			isLeader = false;
		}

	}
	
	
}