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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.seldon.api.resource.DynamicParameterBean;
import io.seldon.api.service.ABTest;
import io.seldon.api.service.ABTestingServer;
import io.seldon.api.service.DynamicParameterServer;
import io.seldon.api.state.ZkABTestingUpdater;
import io.seldon.api.state.ZkCuratorHandler;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.KillSession;

public class TestABTestingUpdater extends BasePeerTest {

	static final String consumerName = "test";
	static final String zkServer = "localhost";

	
	
	@Before
	public void createPath() throws Exception
	{
        new ZkCuratorHandler(zkServer);
	}
	
	@After
	public void tearDown()
	{
		ZkCuratorHandler.shutdown();
	}
	
	@Test @Ignore
	public void testZkUpdaterThread() throws InterruptedException
	{
		ZkABTestingUpdater updaterAB = new ZkABTestingUpdater(consumerName, ZkCuratorHandler.getPeer());
		Thread t = new Thread(updaterAB);
		t.setName("ABTestingUpdater_"+consumerName);
		t.start();
		
	}
	
	@Test
	public void testABTestGetPercentage()
	{
		Random r = new Random();
		CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        alg.setAbTestingKey(""+r.nextInt());
        double percentage = 0.1;
        ABTest p = new ABTest(alg, percentage);
        
        
        ABTestingServer.setABTest(consumerName, p);
        
        
        int inTestCount = 0;
        int noTestCount = 0;
        int numTests = 1000;
        for(int i=0;i<numTests;i++)
        {
        	CFAlgorithm algRet = ABTestingServer.getUserTest(consumerName,null, ""+r.nextInt());
        	if (algRet != null)
        		inTestCount++;
        	else
        		noTestCount++;
        }
        System.out.println("Number of users in AB Test:"+inTestCount);
        Assert.assertEquals(numTests*percentage, inTestCount,50);
        
	}
	
	
	@Test
	public void testABTestGetPercentageSameUser()
	{
		Random r = new Random();
		CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        alg.setAbTestingKey(""+r.nextInt());
        double percentage = 0.5;
        ABTest p = new ABTest(alg, percentage);
        
        
        ABTestingServer.setABTest(consumerName, p);
        
        
        int inTestCount = 0;
        int noTestCount = 0;
        int numTests = 1000;
        String userId = ""+r.nextInt();
        for(int i=0;i<numTests;i++)
        {
        	CFAlgorithm algRet = ABTestingServer.getUserTest(consumerName,null, userId);
        	if (algRet != null)
        		inTestCount++;
        	else
        		noTestCount++;
        }
        System.out.println("Number of users in AB Test:"+inTestCount);
        if (inTestCount > 0)
        	Assert.assertEquals(numTests, inTestCount);
        else
        	Assert.assertEquals(numTests, noTestCount);
	}
	
	
	
	@Test 
	public void testABTestingUpdaterWithReconnect() throws Exception
	{
		ZkABTestingUpdater updater = new ZkABTestingUpdater(consumerName, ZkCuratorHandler.getPeer());
		Thread t = new Thread(updater);
		t.start();

        Thread.sleep(4000); // takes time to connect to zookeeper so allow Thread some time
		
        //force updater to reconnect
        KillSession.kill(ZkCuratorHandler.getPeer().getCurator().getZookeeperClient().getZooKeeper(), zkServer);
        
        Thread.sleep(4000); // takes time to connect to zookeeper so allow Thread some time
        
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        
        CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
        recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
        alg.setRecommenders(recommenders);
        final String abKey = "test1234";
        alg.setAbTestingKey(abKey);
        
        final double percentage = 0.1;
        ABTest p = new ABTest(alg, percentage);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(p);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkABTestingUpdater.AB_ALG_PATH, buf);
        
        client.close();
        
        Thread.sleep(4000);
        
        Assert.assertEquals(1, updater.getNumUpdates()); 
        
        ABTest abTestRet = ABTestingServer.getABTest(consumerName,null);
        
        Assert.assertNotNull(abTestRet);
        
        Assert.assertEquals(p.getAlgorithm(),abTestRet.getAlgorithm());
        Assert.assertEquals(p.getPercentage(),abTestRet.getPercentage());
        
        updater.setKeepRunning(false);
        t.interrupt();
        
	}
	
	@Test 
	public void testABTestingUpdater() throws Exception
	{
		ZkABTestingUpdater updater = new ZkABTestingUpdater(consumerName, ZkCuratorHandler.getPeer());
		Thread t = new Thread(updater);
		t.start();

        Thread.sleep(4000); // takes time to connect to zookeeper so allow Thread some time
		
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        
        CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
        recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
        alg.setRecommenders(recommenders);
        final String abKey = "test1234";
        alg.setAbTestingKey(abKey);
        
        final double percentage = 0.1;
        ABTest p = new ABTest(alg, percentage);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(p);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkABTestingUpdater.AB_ALG_PATH, buf);
        
        client.close();
        
        Thread.sleep(4000);
        
        Assert.assertEquals(1, updater.getNumUpdates());
        
        ABTest abTestRet = ABTestingServer.getABTest(consumerName,null);
        
        Assert.assertNotNull(abTestRet);
        
        Assert.assertEquals(p.getAlgorithm(),abTestRet.getAlgorithm());
        Assert.assertEquals(p.getPercentage(),abTestRet.getPercentage());
        
        updater.setKeepRunning(false);
        t.interrupt();
        
	}
	
	
	@Test 
	public void testABTestingUpdaterMultipleUpdates() throws Exception
	{
		ZkABTestingUpdater updater = new ZkABTestingUpdater(consumerName, ZkCuratorHandler.getPeer());
		Thread t = new Thread(updater);
		t.start();

        Thread.sleep(4000); // takes time to connect to zookeeper so allow Thread some time
		
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        
        CFAlgorithm alg = new CFAlgorithm();
        alg.setName(consumerName);
        List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
        recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
        alg.setRecommenders(recommenders);
        alg.setAbTestingKey("B");

        
        double percentage = 0.1;
        ABTest p = new ABTest(alg, percentage);
        
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(p);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkABTestingUpdater.AB_ALG_PATH, buf);
        
        
        Thread.sleep(4000);
        
        Assert.assertEquals(1, updater.getNumUpdates());
        
        ABTest abTestRet = ABTestingServer.getABTest(consumerName,null);
        
        Assert.assertNotNull(abTestRet);
        
        Assert.assertEquals(p.getAlgorithm(),abTestRet.getAlgorithm());
        Assert.assertEquals(p.getPercentage(),abTestRet.getPercentage());
        
        
        //2nd time
        alg = new CFAlgorithm();
        alg.setName(consumerName);
        recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
        recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_DYNAMIC);
        alg.setAbTestingKey("B");
        alg.setRecommenders(recommenders);
        percentage = 95;
        p = new ABTest(alg, percentage);
        
        
        bos = new ByteArrayOutputStream() ;
        out = new ObjectOutputStream(bos) ;
        out.writeObject(p);
        out.close();

        // Get the bytes of the serialized object
        buf = bos.toByteArray();
        client.setData().forPath(ZkABTestingUpdater.AB_ALG_PATH, buf);
        
        
        Thread.sleep(4000);
        
        Assert.assertEquals(2, updater.getNumUpdates());
        
        abTestRet = ABTestingServer.getABTest(consumerName,null);
        
        Assert.assertNotNull(abTestRet);
        
        Assert.assertEquals(p.getAlgorithm(),abTestRet.getAlgorithm());
        Assert.assertEquals(p.getPercentage(),abTestRet.getPercentage());
        
        client.close();
        
        updater.setKeepRunning(false);
        t.interrupt();
        
	}
	
	
	@Test 
	public void testDynamicParameterUpdater() throws Exception
	{
		ZkABTestingUpdater updater = new ZkABTestingUpdater(consumerName, ZkCuratorHandler.getPeer());
		Thread t = new Thread(updater);
		t.start();

        Thread.sleep(4000); // takes time to connect to zookeeper so allow Thread some time
		
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        

        
        String paramVal = "true";
        DynamicParameterBean b = new DynamicParameterBean(DynamicParameterServer.Parameter.AB_TESTING.name(), paramVal);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(b);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkABTestingUpdater.DYNAMIC_PARAM_PATH, buf);
        
        client.close();
        
        Thread.sleep(2000);
        
        Assert.assertEquals(1, updater.getNumUpdates());
        
        String val = DynamicParameterServer.getParameter(consumerName, DynamicParameterServer.Parameter.AB_TESTING.name());
        
        Assert.assertEquals(paramVal,val);
        updater.setKeepRunning(false);
        t.interrupt();
        
	}
	
	@Test 
	public void testDynamicParameterUpdaterMultipleUpdates() throws Exception
	{
		ZkABTestingUpdater updater = new ZkABTestingUpdater(consumerName, ZkCuratorHandler.getPeer());
		Thread t = new Thread(updater);
		t.start();
		
		Thread.sleep(4000); // takes time to connect to zookeeper so allow Thread some time
		
		CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        CuratorFramework client = builder.connectString(zkServer).namespace(consumerName).retryPolicy(new RetryOneTime(1)).build();
        client.start();
        
        String paramVal = "true";
        DynamicParameterBean b = new DynamicParameterBean(DynamicParameterServer.Parameter.AB_TESTING.name(), paramVal);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
        ObjectOutput out = new ObjectOutputStream(bos) ;
        out.writeObject(b);
        out.close();

        // Get the bytes of the serialized object
        byte[] buf = bos.toByteArray();
        client.setData().forPath(ZkABTestingUpdater.DYNAMIC_PARAM_PATH, buf);
        
        
        Thread.sleep(4000);
        
        Assert.assertEquals(1, updater.getNumUpdates());
        
        String val = DynamicParameterServer.getParameter(consumerName, DynamicParameterServer.Parameter.AB_TESTING.name());
        
        Assert.assertEquals(paramVal,val);
        
        paramVal = "false";
        b = new DynamicParameterBean(DynamicParameterServer.Parameter.AB_TESTING.name(), paramVal);
        
        bos = new ByteArrayOutputStream() ;
        out = new ObjectOutputStream(bos) ;
        out.writeObject(b);
        out.close();

        // Get the bytes of the serialized object
        buf = bos.toByteArray();
        client.setData().forPath(ZkABTestingUpdater.DYNAMIC_PARAM_PATH, buf);
        

        
        Thread.sleep(4000);
        
        Assert.assertEquals(2, updater.getNumUpdates());
        
        val = DynamicParameterServer.getParameter(consumerName, DynamicParameterServer.Parameter.AB_TESTING.name());
        
        Assert.assertEquals(paramVal,val);
        
        client.close();
        updater.setKeepRunning(false);
        t.interrupt();
	}
}
