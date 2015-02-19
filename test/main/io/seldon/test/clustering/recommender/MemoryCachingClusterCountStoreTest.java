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

package io.seldon.test.clustering.recommender;

import java.util.ArrayList;
import java.util.List;

import io.seldon.clustering.recommender.MemoryClusterCountStore;
import junit.framework.Assert;
import org.junit.Test;

public class MemoryCachingClusterCountStoreTest {

	@Test
	public void testMultipleIncrementsNotExponential() throws InterruptedException
	{
		final int numThreads = 100;
		final int numIterations = 1000;
		final double weight = 1D;
		final long timestamp = 1;
		final MemoryClusterCountStore store = new MemoryClusterCountStore(MemoryClusterCountStore.COUNTER_TYPE.SIMPLE,2,0,1);
		List<Thread> threads = new ArrayList<>();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int j=0;j<numIterations;j++)
					store.add(1, 1, weight,timestamp);
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		Assert.assertEquals(numThreads*numIterations*weight, store.getCount(1, 1, timestamp,0));
	}
	
	@Test
	/**
	 * Basic test with no decay as all entries added at same time
	 */
	public void testMultipleIncrementsExponentialNoDecay() throws InterruptedException
	{
		final int numThreads = 100;
		final int numIterations = 1000;
		final double weight = 1D;
		final long timestamp = 1;
		final long time = 1;
		final MemoryClusterCountStore store = new MemoryClusterCountStore(MemoryClusterCountStore.COUNTER_TYPE.DECAY,2,0D,1D);
		List<Thread> threads = new ArrayList<>();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int j=0;j<numIterations;j++)
					store.add(1, 1, weight,timestamp,time);
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		Assert.assertEquals(numThreads*numIterations*weight, store.getCount(1, 1, timestamp,time));
	}
	
	
	@Test
	/**
	 * 
	 */
	public void testMultipleIncrementsExponential() throws InterruptedException
	{
		final int numThreads = 100;
		final int numIterations = 1000;
		final double weight = 1D;
		final long timestamp = 1;
		final long time = 1;
		final MemoryClusterCountStore store = new MemoryClusterCountStore(MemoryClusterCountStore.COUNTER_TYPE.DECAY,2,0D,1D);
		List<Thread> threads = new ArrayList<>();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int j=0;j<numIterations;j++)
					store.add(1, 1, weight,timestamp,time);
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		Thread.sleep(2000); // add a delay before getting value
		Assert.assertTrue(numThreads*numIterations*weight > store.getCount(1, 1, timestamp,time)/2);
	}
	
	
	@Test
	public void testMultipleWeightedIncrementsNotExponential() throws InterruptedException
	{
		final int numThreads = 100;
		final int numIterations = 1000;
		final double weight = 0.5D;
		final long timestamp = 1;
		final MemoryClusterCountStore store = new MemoryClusterCountStore(MemoryClusterCountStore.COUNTER_TYPE.SIMPLE,2,0,1);
		List<Thread> threads = new ArrayList<>();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int j=0;j<numIterations;j++)
					store.add(1, 1, weight,timestamp);
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		Assert.assertEquals(numThreads*numIterations*weight, store.getCount(1, 1, timestamp,0));
	}
	
}
