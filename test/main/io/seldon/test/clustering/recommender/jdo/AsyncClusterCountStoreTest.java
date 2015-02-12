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

package io.seldon.test.clustering.recommender.jdo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.recommender.jdo.AsyncClusterCountStore;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class AsyncClusterCountStoreTest extends BasePeerTest {

	@Autowired PersistenceManager pm;
	
	@Autowired
	GenericPropertyHolder props;
	
	private void clearClusterCounts()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from  cluster_counts");
					query.execute();
				}
			});
		} catch (DatabaseException e)
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private Long getNumberCounts()
	{
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select count(*) from cluster_counts");
		query.setResultClass(Long.class);
		query.setUnique(true);
		Long res = (Long) query.execute();
		query.closeAll();
		return res;
	}
	
	@Test
	public void testAddCounts() throws InterruptedException
	{
		Random rand = new Random();
		AsyncClusterCountStore asyncStore = new AsyncClusterCountStore(props.getClient(), 1, 25000000, Integer.MAX_VALUE, 3, 1,false);
		
		Thread queueRunner = new Thread(asyncStore);
		queueRunner.start();
		
		AsyncClusterCountStore.ClusterCount cc = new AsyncClusterCountStore.ClusterCount(1,1L,1L,1D);
		asyncStore.put(cc);
		cc = new AsyncClusterCountStore.ClusterCount(1,1L,2L,1D);
		Thread.sleep(2000);
		asyncStore.put(cc);
		Thread.sleep(2000);
		double count = clusterCount.getCount(1, 1,1,2L);
		Assert.assertEquals(1.367D, count,0.1);

		//cleanup
		clearClusterCounts();
	}
	
	@Test
	public void testAddCountsBadTime() throws InterruptedException
	{
		Random rand = new Random();
		AsyncClusterCountStore asyncStore = new AsyncClusterCountStore(props.getClient(), 1, 25000000, Integer.MAX_VALUE, 3, 1,false);
		
		Thread queueRunner = new Thread(asyncStore);
		queueRunner.start();
		
		AsyncClusterCountStore.ClusterCount cc = new AsyncClusterCountStore.ClusterCount(1,1L,2L,1D);
		asyncStore.put(cc);
		cc = new AsyncClusterCountStore.ClusterCount(1,1L,1L,1D);//time before first time
		asyncStore.put(cc);
		Thread.sleep(2000);
		double count = clusterCount.getCount(1, 1,1,2L);
		Assert.assertEquals(2D, count,0.1);

		//cleanup
		clearClusterCounts();
	}
	
	@Test 
	public void checkContinuousWrites() throws InterruptedException
	{
		Random rand = new Random();
		AsyncClusterCountStore asyncStore = new AsyncClusterCountStore(props.getClient(), 1, 25000000, Integer.MAX_VALUE, 3, 3600,true);
		
		Thread queueRunner = new Thread(asyncStore);
		queueRunner.start();
		
		long numInserts = 100;
		long timeBase = System.currentTimeMillis()/1000;
		for(int i=0;i<numInserts;i++)
		{
			AsyncClusterCountStore.ClusterCount cc = new AsyncClusterCountStore.ClusterCount(i,i,timeBase+i,rand.nextDouble());
			asyncStore.put(cc);
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		Thread.sleep(3000);

		Long numCounts = getNumberCounts();
		Assert.assertNotNull(numCounts);
		Assert.assertEquals(numInserts, (long)numCounts);
	
		//cleanup
		clearClusterCounts();
	}
	
	
	@Test 
	public void checkContinuousWritesSingleCluster() throws InterruptedException
	{

		Random rand = new Random();
		AsyncClusterCountStore asyncStore = new AsyncClusterCountStore(props.getClient(), 1, 25000000, Integer.MAX_VALUE, 3, 3600,true);
		
		Thread queueRunner = new Thread(asyncStore);
		queueRunner.start();
		
		long numInserts = 100;
		long timeBase = System.currentTimeMillis()/1000;
		for(int i=0;i<numInserts;i++)
		{
			AsyncClusterCountStore.ClusterCount cc = new AsyncClusterCountStore.ClusterCount(1,i,timeBase+i,rand.nextDouble());
			asyncStore.put(cc);
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		Thread.sleep(3000);

		Long numCounts = getNumberCounts();
		Assert.assertNotNull(numCounts);
		Assert.assertEquals(numInserts, (long)numCounts);
		
		//cleanup
		clearClusterCounts();

	}
	
	@Test 
	public void checkContinuousWritesSingleClusterSingleItem() throws InterruptedException
	{
	
		Random rand = new Random();
		AsyncClusterCountStore asyncStore = new AsyncClusterCountStore(props.getClient(), 1, 25000000, Integer.MAX_VALUE, 3, 360000000,true);
		
		Thread queueRunner = new Thread(asyncStore);
		queueRunner.start();
		
		long numInserts = 100;
		long timeBase = System.currentTimeMillis()/1000;
		for(int i=0;i<numInserts;i++)
		{
			AsyncClusterCountStore.ClusterCount cc = new AsyncClusterCountStore.ClusterCount(1,1,timeBase+i,1);
			asyncStore.put(cc);
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		Thread.sleep(3000);

		Long numCounts = getNumberCounts();
		Assert.assertNotNull(numCounts);
		Assert.assertEquals(1, (long)numCounts);
		double count = clusterCount.getCount(1, 1, 1, System.currentTimeMillis()/1000);
		Assert.assertEquals(numInserts, count, 0.1);
		
		
		//cleanup
		clearClusterCounts();

	}
	
	@Test 
	public void checkContinuousWritesSingleClusterSingleItemMultipleThreads() throws InterruptedException
	{
		int numThreads = 40;
		List<Thread> threads = new ArrayList<Thread>();
		Random rand = new Random();
		final AsyncClusterCountStore asyncStore = new AsyncClusterCountStore(props.getClient(), 1, 1000, Integer.MAX_VALUE, 3, 360000000,true);
		
		Thread queueRunner = new Thread(asyncStore);
		queueRunner.start();
		
		final long numInserts = 100;
		final long timeBase = System.currentTimeMillis()/1000;
		
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int i=0;i<numInserts;i++)
				{
					AsyncClusterCountStore.ClusterCount cc = new AsyncClusterCountStore.ClusterCount(1,1,timeBase+i,1);
					asyncStore.put(cc);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		
		System.out.println("Sleeping...");
		Thread.sleep(3000);

		Long numCounts = getNumberCounts();
		Assert.assertNotNull(numCounts);
		Assert.assertEquals(1, (long)numCounts);
		double count = clusterCount.getCount(1, 1, 1, System.currentTimeMillis()/1000);
		Assert.assertEquals(numInserts*numThreads, count, 0.3);
		
		//cleanup
		clearClusterCounts();

		
	}
	
	@Test 
	public void checkContinuousWritesSingleClusterMultipleThreads() throws InterruptedException
	{
		int numThreads = 40;
		List<Thread> threads = new ArrayList<Thread>();
	
		
		final AsyncClusterCountStore asyncStore = new AsyncClusterCountStore(props.getClient(), 1, 10000, Integer.MAX_VALUE, 3, 360000000,true);
		
		Thread queueRunner = new Thread(asyncStore);
		queueRunner.start();
		
		final long numInserts = 100;
		final long timeBase = System.currentTimeMillis()/1000;
		
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				Random rand = new Random();
				for(int i=0;i<numInserts;i++)
				{
					AsyncClusterCountStore.ClusterCount cc = new AsyncClusterCountStore.ClusterCount(1,rand.nextInt(20),timeBase+i,1);
					asyncStore.put(cc);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		
		System.out.println("Sleeping...");
		Thread.sleep(3000);

		Long numCounts = getNumberCounts();
		Assert.assertNotNull(numCounts);
		Assert.assertEquals(20, (long)numCounts,3);

		//cleanup
		clearClusterCounts();

	}
	
}
