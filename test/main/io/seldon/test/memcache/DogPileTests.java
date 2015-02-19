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


import io.seldon.memcache.DogpileHandler;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DogPileTests {

	@Before
	public void setup()
	{
		new DogpileHandler("0.75","true");
	}
	
	@Test
	public void basicFunctionalityTest() throws InterruptedException
	{
		final String key = "testKey";
		final int expireSecs = 5;
		Assert.assertTrue(DogpileHandler.get().updateIsRequired(key, new Object(), expireSecs)); // no key set so no need to update
		DogpileHandler.get().updated(key, expireSecs);
		Assert.assertFalse(DogpileHandler.get().updateIsRequired(key, new Object(), expireSecs)); // key set but not expired
		Thread.sleep(expireSecs*1000);
		Assert.assertTrue(DogpileHandler.get().updateIsRequired(key, new Object(), expireSecs)); // key set and expired
		Assert.assertFalse(DogpileHandler.get().updateIsRequired(key, new Object(), expireSecs)); // key set and expired but waiting for update
		DogpileHandler.get().clear(key);
	}
	
	@Test
	public void noKeyTest() throws InterruptedException
	{
		final String key = "testKey";
		final int expireSecs = 5;
		DogpileHandler.get().clear(key);

		Assert.assertTrue(DogpileHandler.get().updateIsRequired(key, new Object(), expireSecs)); // no key set so no need to update
		DogpileHandler.get().updated(key, expireSecs);
		Assert.assertFalse(DogpileHandler.get().updateIsRequired(key, new Object(), expireSecs)); // key set but not expired
		Thread.sleep(expireSecs*1000);
		Assert.assertTrue(DogpileHandler.get().updateIsRequired(key, new Object(), expireSecs)); // key set and expired
		Assert.assertFalse(DogpileHandler.get().updateIsRequired(key, new Object(), expireSecs)); // key set and expired but waiting for update
		DogpileHandler.get().clear(key);
	}
	
	public static class UpdaterTester implements Runnable
	{
		int updates = 0;
		int delay;
		String key;
		int keyExpireSecs;
		boolean keepRunning = true;
		public UpdaterTester(int delayMillisecs,String key,int keyExpireSecs)
		{
			this.delay = delayMillisecs;
			this.key = key;
			this.keyExpireSecs = keyExpireSecs;
		}
		
		@Override
		public void run() {

			while(keepRunning)
			{
				boolean needsUpdate = DogpileHandler.get().updateIsRequired(key, new Object(), 5);
				if (needsUpdate)
				{
					System.out.println("Update "+System.currentTimeMillis());
					updates++;
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					DogpileHandler.get().updated(key, keyExpireSecs);
				}
				try {
					Thread.sleep(this.delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
					return;
				}
			}
		}

		public int getUpdates() {
			return updates;
		}

		public void setKeepRunning(boolean keepRunning) {
			this.keepRunning = keepRunning;
		}
		
		
	}
	
	@Test 
	public void multiThreadedTest() throws InterruptedException
	{
		
		int numThreads = 100;
		final String key = "testKey";
		try
		{
		final int expireSecs = 5;
		final int busyLoopDelayMillisecs = 100;
		List<Thread> threads = new ArrayList<>();
		List<UpdaterTester> testers = new ArrayList<>();
		DogpileHandler.get().updated(key, expireSecs);
		System.out.println("Start test "+System.currentTimeMillis());
		for(int i=0;i<numThreads;i++)
		{
			UpdaterTester ut = new UpdaterTester(busyLoopDelayMillisecs, key, expireSecs);
			Thread t = new Thread(ut);
			threads.add(t);
			testers.add(ut);
			t.start();
		}
		
		Thread.sleep(expireSecs * 1000);
		
		for(UpdaterTester t : testers)
			t.setKeepRunning(false);
		
		System.out.println("End test "+System.currentTimeMillis());
		
		for(Thread t : threads)
			t.join();
		
		int sum = 0;
		for(UpdaterTester t : testers)
			sum = sum + t.getUpdates();
		
		Assert.assertEquals(1, sum);
		}
		finally
		{
			DogpileHandler.get().clear(key);
		}
	}
}
