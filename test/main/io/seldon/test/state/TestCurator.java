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

package io.seldon.test.state;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.retry.RetryOneTime;

public class TestCurator {

	
	 @Test @Ignore
	    public void testNamespaceInBackground() throws Exception
	    {
	        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
	        CuratorFramework client = builder.connectString("localhost").namespace("aisa").retryPolicy(new RetryOneTime(1)).build();
	        client.start();
	        try
	        {
	            final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
	            CuratorListener listener = new CuratorListener()
	            {
	                @Override
	                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
	                {
	                    if ( event.getType() == CuratorEventType.EXISTS )
	                    {
	                        queue.put(event.getPath());
	                    }
	                }
	            };

	            client.getCuratorListenable().addListener(listener);
	            //client.create().forPath("/base");
	            client.checkExists().inBackground().forPath("/base");

	            String path = queue.poll(10, TimeUnit.SECONDS);
	            Assert.assertEquals(path, "/base");

	            client.getCuratorListenable().removeListener(listener);

	            BackgroundCallback callback = new BackgroundCallback()
	            {
	                @Override
	                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
	                {
	                    queue.put(event.getPath());
	                }
	            };
	            client.getChildren().inBackground(callback).forPath("/base");
	            path = queue.poll(10, TimeUnit.SECONDS);
	            Assert.assertEquals(path, "/base");
	        }
	        finally
	        {
	            client.close();
	        }
	    }

	 
	 
	
	@Test @Ignore
	public void connectToZookeeper()
	{
		try
		{
			//CuratorFramework    client = CuratorFrameworkFactory.newClient("localhost", 1000, 1000, new RetryOneTime(1));
			
			CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
	        CuratorFramework client = builder.connectString("localhost").namespace("aisa").retryPolicy(new RetryOneTime(1)).build();
	        client.start();
			
			final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
            CuratorListener listener = new CuratorListener()
            {
                @Override
                public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
                {
                	if (event.getType() == CuratorEventType.GET_DATA)
                	{
                		queue.put(new String(event.getData(),"UTF-8"));
                	}
                	else if ( event.getType() == CuratorEventType.EXISTS )
                    {
                        queue.put(event.getPath());
                    }
                }
            };
            
            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    try
                    {
                        queue.put(event.getPath());
                    }
                    catch ( InterruptedException e )
                    {
                        throw new Error(e);
                    }
                }
            };
			
			System.out.println("Connected");
			client.getCuratorListenable().addListener(listener);
			
			String node = "/test1";

			//client.create().forPath(node);
			client.create().withMode(CreateMode.EPHEMERAL).forPath(node, "some data".getBytes());
			
			System.out.println("Created node "+node);
			
			if (client.checkExists().forPath("/gggg") != null)
				System.out.println("Exists ");
			else
				System.out.println("does not exist");
			
			String path = queue.poll(10, TimeUnit.SECONDS);
			System.out.println("Path is "+path);
			
			client.getData().inBackground().forPath(node);

			String data = queue.poll(10, TimeUnit.SECONDS);
			System.out.println("Data is "+data);


			SetZKData setter = new SetZKData(node, "some new data");
			Thread t = new Thread(setter);
			t.start();
			
			for(int i=0;i<10;i++)
			{	
				System.out.println("Sleeping...");
				Thread.sleep(8000);
			
				client.getData().usingWatcher(watcher).forPath(node);
			
				path = queue.poll(10, TimeUnit.SECONDS);
				System.out.println(" Path changed "+path);
			
				client.getData().inBackground().forPath(node);

				data = queue.poll(10, TimeUnit.SECONDS);
				System.out.println("Data is now"+data);
			}
			
			setter.setKeepRunning(false);
			
			client.getCuratorListenable().removeListener(listener);
	            
			client.close();
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{}
	}
	
	public class SetZKData implements Runnable
	{

		String path;
		String data;
		
		boolean keepRunning = true;
		



		public SetZKData(String path, String data) {
			super();
			this.path = path;
			this.data = data;
		}






		public boolean isKeepRunning() {
			return keepRunning;
		}






		public void setKeepRunning(boolean keepRunning) {
			this.keepRunning = keepRunning;
		}






		@Override
		public void run() 
		{
			
			try
			{
				//CuratorFramework    client = CuratorFrameworkFactory.newClient("localhost", 1000, 1000, new RetryOneTime(1));
				

				CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
		        CuratorFramework client = builder.connectString("localhost").namespace("aisa").retryPolicy(new RetryOneTime(1)).build();
		        client.start();
		        
		        int count = 1;
		        
		        while(keepRunning)
		        {
		        	//System.out.println("Sleeping...");
					Thread.sleep(2000);		        	
					String dataNew = data+count;
			        //System.out.println("Setting "+path+" to "+dataNew);
			        client.setData().forPath(path, dataNew.getBytes());
			        count++;

		        }
		        
		        client.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally{}
		}
		
	}

}
