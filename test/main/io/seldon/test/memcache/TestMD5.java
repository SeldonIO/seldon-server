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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import io.seldon.memcache.SecurityHashPeer;
import junit.framework.Assert;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

public class TestMD5 {

	@Before
	public void setup() throws NoSuchAlgorithmException
	{
		SecurityHashPeer.initialise();
		SingleMD5.initialise();
	}

	@Test
	public void testMD5() throws NoSuchAlgorithmException
	{
		String stuff = "safaf afafas fasf saf sa fsa fsa f saf safasewwegfewgew";
		long start = System.currentTimeMillis();
		for(int i=0;i<10000;i++)
		{
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			byte[] result =  md5.digest( stuff.getBytes() );
		}
		long end = System.currentTimeMillis();
		System.out.println("New Instance MD5 Time "+(end-start));


		start = System.currentTimeMillis();
		MessageDigest md5 = MessageDigest.getInstance("MD5");
		for(int i=0;i<10000;i++)
		{

			byte[] result =  md5.digest( stuff.getBytes() );
		}
		end = System.currentTimeMillis();
		System.out.println("Single MD5 Time "+(end-start));

	}

	@Test
	public void testMultiThreadedMD5() throws InterruptedException
	{
		final String stuff = "safaf afafas fasf saf sa fsa fsa f saf safasewwegfewgew";
		final int numThreads = 200;
		final int numIterations = 10000;
		List<Thread> threads = new ArrayList<>();

		long start = System.currentTimeMillis();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int i=0;i<numIterations;i++)
				{
					String hashed = SecurityHashPeer.md5(stuff);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
        fireThreads(threads);
        long end = System.currentTimeMillis();
		long time1 = end-start;
		System.out.println("SecurityHashPeer MD5 Time "+time1);

		start = System.currentTimeMillis();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int i=0;i<numIterations;i++)
				{
					String hashed = SingleMD5.md5(stuff);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
        fireThreads(threads);
        end = System.currentTimeMillis();
		long time2 = end - start;
		System.out.println("Single MD5 Time "+time2);

        start = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            Runnable r = new Runnable() {
                public void run() {
                    for (int i = 0; i < numIterations; i++) {
                        SecurityHashPeer.md5digest(stuff);
                    }
                }
            };
            Thread t = new Thread(r);
            threads.add(t);
            t.start();
        }
        fireThreads(threads);
        end = System.currentTimeMillis();
        long time3 = end - start;
        System.out.println("DigestUtils MD5 Time " + time3);

        // This should no longer cause the test to fail,
        //   Assert.assertTrue("Single MD5 synchronized is slower than multi threaded with constructor", time2 > time1);
        Assert.assertTrue("DigestUtils MD5 should be faster than synchronized block.", time3 < time1);
	}

    private void fireThreads(List<Thread> threads) throws InterruptedException {
        for (Thread t : threads) {
            t.join();
        }
    }


    public static class SingleMD5
	{
		static MessageDigest md5;
		public static void initialise() throws NoSuchAlgorithmException
		{
			md5 = MessageDigest.getInstance("MD5");
		}

		 public static synchronized String md5(String value)
	     {
	        if (md5 != null)
	        {
	           byte[] result =  md5.digest( value.getBytes() );
	           return hexEncode(result);
       	 	}
	        else
	        {
            	final String message = "Can't get md5";
	           return "";
	       }
	     }

		 static public String hexEncode( byte[] aInput)
		 {
				StringBuffer result = new StringBuffer();
				final char[] digits = {'0', '1', '2', '3', '4','5','6','7','8','9','a','b','c','d','e','f'};
				for ( int idx = 0; idx < aInput.length; ++idx) {
				  byte b = aInput[idx];
				  result.append( digits[ (b&0xf0) >> 4 ] );
				  result.append( digits[ b&0x0f] );
				}
				return result.toString();
		 }
	}

    @Test
    public void consistentEncoding() {
        for (int i = 0; i < 1000; i++) {
            String randomId = RandomStringUtils.random(24);
            Assert.assertEquals(SecurityHashPeer.md5digest(randomId), SecurityHashPeer.md5(randomId));
        }
    }

}
