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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import io.seldon.api.AlgorithmService;
import io.seldon.api.Util;
import io.seldon.api.state.ZkAlgorithmUpdater;
import io.seldon.api.state.ZkCuratorHandler;
import io.seldon.trust.impl.CFAlgorithm;
import junit.framework.Assert;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.KillSession;
import io.seldon.api.AlgorithmServiceImpl;

public class TestAlgorithmOptionsUpdater {

	static final String consumerName = "test";
	static final String zkServer = "localhost";
	
	AlgorithmService origService;
	
	@Before
	public void createPath() throws Exception
	{
        new ZkCuratorHandler(zkServer);
		
		origService = Util.getAlgorithmService();

        AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<String, CFAlgorithm>();
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
	}
	
	@After
	public void restoreAlgService()
	{
		Util.setAlgorithmService(origService);
		
		ZkCuratorHandler.shutdown();
	}
	int updates = 0;
	
	
	@Test @Ignore
	public void testNodeCache() throws Exception
	{
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
			
			@Override
			public void childEvent(CuratorFramework arg0, PathChildrenCacheEvent arg1)
					throws Exception {
				System.out.println("Child changed");
				
			}
		};
        
        PathChildrenCache nodeCache = new PathChildrenCache(client, ZkAlgorithmUpdater.ALG_PATH,true);
        nodeCache.start();
        nodeCache.getListenable().addListener(listener);
        
        CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
        recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
        alg.setRecommenders(recommenders);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(alg);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkAlgorithmUpdater.ALG_PATH, buf);
        
        Thread.sleep(1000);
        
        ChildData data = nodeCache.getCurrentData(ZkAlgorithmUpdater.ALG_PATH);
        ObjectInputStream  in = new ObjectInputStream(new ByteArrayInputStream(data.getData()));
        CFAlgorithm  alg2 = (CFAlgorithm) in.readObject();
        in.close();
        
        Assert.assertEquals(alg, alg2);
        nodeCache.close();
        client.close();
	}
	
	@Test @Ignore
	public void multipleWatches() throws Exception
	{
		
		final Watcher watcher = new Watcher()
			{
				
				@Override
				public void process(WatchedEvent event)
				{
					updates++;
				}
				
				
			};
			
			
			
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        
        client.getData().usingWatcher(watcher).forPath(ZkAlgorithmUpdater.ALG_PATH);
        client.getData().usingWatcher(watcher).forPath(ZkAlgorithmUpdater.ALG_PATH);
   
        
        CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
        recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
        alg.setRecommenders(recommenders);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(alg);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkAlgorithmUpdater.ALG_PATH, buf);
        
        client.close();
        
        Thread.sleep(4000);
        
        Assert.assertEquals(2, updates);
       
	}
	
	@Test
	public void testUpdater() throws Exception
	{
		
		System.out.println("Start testUpdater in test AlgorithmsOptionsUpdater");
		ZkAlgorithmUpdater updater = new ZkAlgorithmUpdater(consumerName, ZkCuratorHandler.getPeer());
		Thread t = new Thread(updater);
		t.start();
		
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        
        Thread.sleep(4000);
        
        CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
        recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
        alg.setRecommenders(recommenders);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(alg);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkAlgorithmUpdater.ALG_PATH, buf);
        
        Thread.sleep(3000);
        
        Assert.assertEquals(1, updater.getNumUpdates());
        
        CFAlgorithm algNew = Util.getAlgorithmService().getAlgorithmOptions(consumerName);
        
        Assert.assertEquals(alg, algNew);
       
        client.close();
		
	}
	
	
	@Test
	public void testUpdaterRestart() throws Exception
	{
		System.out.println("Start testUpdater in test AlgorithmsOptionsUpdater");
		ZkAlgorithmUpdater updater = new ZkAlgorithmUpdater(consumerName, ZkCuratorHandler.getPeer());
		Thread t = new Thread(updater);
		t.start();
		
		Thread.sleep(10000);
		
		KillSession.kill(ZkCuratorHandler.getPeer().getCurator().getZookeeperClient().getZooKeeper(), zkServer);
		
		Thread.sleep(4000);
		
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        
        CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
        recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
        alg.setRecommenders(recommenders);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(alg);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkAlgorithmUpdater.ALG_PATH, buf);

        Thread.sleep(3000);

        Assert.assertEquals(1, updater.getNumUpdates()); 
        
        CFAlgorithm algNew = Util.getAlgorithmService().getAlgorithmOptions(consumerName);
        
        Assert.assertEquals(alg, algNew);

        
        client.close();
        
        
	}
	
	

}
