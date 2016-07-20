/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.stream.itemsim;

import io.seldon.stream.itemsim.MinHashCollections.State;
import io.seldon.stream.itemsim.minhash.Hasher;
import io.seldon.stream.itemsim.minhash.SimplePrimeHash;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class MinHashCollectionsTest {

	
	@Test
	public void test()
	{
		Set<Hasher> existing = new HashSet<>();
		for(int i=0;i<100;i++)
		{
			Hasher h = SimplePrimeHash.create(existing);
			existing.add(h);
		}
		List<Hasher> hashes = new ArrayList<Hasher>(existing);
		MinHasherFactory f = new RollingWindowedMinHashFactory(hashes);
		MinHashCollections mhcs = new MinHashCollections(f, 100, 0);
		for(int i=1;i<100;i++)
		{
			mhcs.add(1, i, i);
			if (i>50)
				mhcs.add(2, i, i);
		}
		List<State> states = mhcs.getAllMinHashes(10);
		for(State s1 : states)
			for(State s2 : states)
			{
				if (s1.id < s2.id)
				{
					float jaccard = s1.jaccardEstimate(s2);
					Assert.assertEquals(0.5, jaccard, 0.15);
					System.out.println(""+s1.id+"->"+s2.id+" "+jaccard);
				}
			}
	}
	
	@Test
	public void testMinActivity()
	{
		Set<Hasher> existing = new HashSet<>();
		for(int i=0;i<100;i++)
		{
			Hasher h = SimplePrimeHash.create(existing);
			existing.add(h);
		}
		List<Hasher> hashes = new ArrayList<Hasher>(existing);
		MinHasherFactory f = new RollingWindowedMinHashFactory(hashes);
		MinHashCollections mhcs = new MinHashCollections(f, 100, 200);
		for(int i=1;i<100;i++)
		{
			mhcs.add(1, i, i);
			if (i>50)
				mhcs.add(2, i, i);
		}
		List<State> states = mhcs.getAllMinHashes(10);
		Assert.assertEquals(0, states.size());
	}
	
	@Test
	public void speedTest()
	{
		Set<Hasher> existing = new HashSet<>();
		for(int i=0;i<100;i++)
		{
			Hasher h = SimplePrimeHash.create(existing);
			existing.add(h);
		}
		List<Hasher> hashes = new ArrayList<Hasher>(existing);
		MinHasherFactory f = new RollingWindowedMinHashFactory(hashes);
		MinHashCollections mhcs = new MinHashCollections(f, 10000, 0);
		Random r = new Random();
		long s1 = System.currentTimeMillis();
		int timeSteps = 100000;
		for(int i=1;i<timeSteps;i++)
		{
			int itemId = r.nextInt(200);
			int userId = r.nextInt(999999);
			mhcs.add(itemId, userId, i);
		}
		long s2 = System.currentTimeMillis();
		long t = s2-s1;
		double reqPerSec = timeSteps/(t/1000.0);
		System.out.println("time:"+t+" per sec "+reqPerSec);
	}
}
