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

package io.seldon.test.memcache;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.seldon.memcache.MemCachePeer;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import net.spy.memcached.CASMutation;

import org.junit.Test;

public class MemCachePeerTest extends BasePeerTest {

	@Test 
	public void testCAS()
	{

		final String x = "1";

		// This is how we modify a list when we find one in the cache.
	    CASMutation<List<String>> mutation = new CASMutation<List<String>>() {

	        // This is only invoked when a value actually exists.
	        public List<String> getNewValue(List<String> current) {
	             current.add(x);
	             return current;
	        }

	    };
	    
	    List<String> initial = new ArrayList<>();
	    initial.add(x);
	    
	    Random r = new Random();
	    String key = ""+r.nextInt();
		MemCachePeer.cas(key, mutation, initial);
		
		List<String> res = (List<String>) MemCachePeer.get(key);
		
		Assert.assertNotNull(res);
		Assert.assertEquals(1, res.size());
	}
	
	
	@Test 
	public void testCASMultiThread() throws InterruptedException
	{
		  Random rand = new Random();
		  final String key = ""+rand.nextInt();
		  try
		  {
			  final String x = "1";

			  final  CASMutation<List<String>> mutation = new CASMutation<List<String>>() {

		        // This is only invoked when a value actually exists.
		        public List<String> getNewValue(List<String> current) {
		             current.add(x);
		             return current;
		        }

		    };
		    
		    int numThreads = 10;
		    final int numIterations = 10;
		  
		    List<Thread> threads = new ArrayList<>();
			long start = System.currentTimeMillis();
			MemCachePeer.delete(key);
			for(int i=0;i<numThreads;i++)
			{
				Runnable r = new Runnable() { public void run()
				{
					long total = 0;				
					for(int j=0;j<numIterations;j++)
					{
						
						 List<String> initial = new ArrayList<>();
						 initial.add(x);
						 MemCachePeer.cas(key, mutation, initial,1000);
					}
					System.out.println("Avg Time:"+(total/(double)numIterations));
				}
				};
				Thread t = new Thread(r);
				threads.add(t);
				t.start();
			}
			
			for(Thread t : threads)
				t.join();
			
			Thread.sleep(2000);
			
			List<String> res = (List<String>) MemCachePeer.get(key);
			
			Assert.assertEquals(numThreads*numIterations, res.size());
		}
		finally
		{
			MemCachePeer.delete(key);
		}
	}
	
	@Test 
	public void testCASTimeout() throws InterruptedException
	{

		final String x = "1";

		// This is how we modify a list when we find one in the cache.
	    CASMutation<List<String>> mutation = new CASMutation<List<String>>() {

	        // This is only invoked when a value actually exists.
	        public List<String> getNewValue(List<String> current) {
	             current.add(x);
	             return current;
	        }

	    };
	    
	    List<String> initial = new ArrayList<>();
	    initial.add(x);
	    Random r = new Random();
	    String key = ""+r.nextInt();
		MemCachePeer.cas(key, mutation, initial,1);
		
		Thread.sleep(2000);
		
		List<String> res = (List<String>) MemCachePeer.get(key);
		
		Assert.assertNull(res);
	}
	
	@Test
	public void testMultiThreadsGet() throws InterruptedException
	{
		int numThreads = 500;
		final int numIterations = 10000;
		
		MemCachePeer.put("mykey", 10L);
		
		List<Thread> threads = new ArrayList<>();
		long start = System.currentTimeMillis();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				long total = 0;				
				for(int j=0;j<numIterations;j++)
				{
					long t1 = System.currentTimeMillis();
					Long l = (Long) MemCachePeer.get("mykey");
					long t2 = System.currentTimeMillis();
					total = total + (t2-t1);
					
				}
				System.out.println("Avg Time:"+(total/(double)numIterations));
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
	}
	
}
