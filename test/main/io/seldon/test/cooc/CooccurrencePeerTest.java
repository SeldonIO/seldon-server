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

package io.seldon.test.cooc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import io.seldon.cooc.CooccurrenceCount;
import io.seldon.cooc.CooccurrencePeer;
import io.seldon.cooc.ICooccurrenceStore;

public class CooccurrencePeerTest {

	
	@Test
	public void testZeroCounts()
	{
		CooccurrencePeer p = new CooccurrencePeer("TEST",100,Long.MAX_VALUE);
		Map<String,CooccurrenceCount> map = new HashMap<>();
		
		ICooccurrenceStore store = new MockCooccurrenceStore(map);
		
		List<Long> item11 = new ArrayList<>();
		for(int i=0;i<5;i++)
			item11.add((long)i);
		
		Map<String,CooccurrenceCount> counts = p.getCountsAA(item11, store);
		
		Assert.assertEquals(item11.size(),counts.size());
		for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			Assert.assertEquals(new CooccurrenceCount(0D,0), e.getValue());
		
		Map<String,CooccurrenceCount> countsAB = p.getCountsAB(item11, item11, store);
		
		Assert.assertEquals(15,countsAB.size());
		for(Map.Entry<String, CooccurrenceCount> e : countsAB.entrySet())
			Assert.assertEquals(new CooccurrenceCount(0D,0), e.getValue());
		
		
	}
	
	@Test 
	public void testCacheSize() throws InterruptedException
	{
		CooccurrencePeer p = new CooccurrencePeer("TEST",1,Long.MAX_VALUE);
		Map<String,CooccurrenceCount> map = new HashMap<>();
		for(int i1 = 0;i1<5;i1++)
			for(int i2=0;i2<5;i2++)
				map.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(1,0));
		MockCooccurrenceStore store = new MockCooccurrenceStore(map);
		
		List<Long> item11 = new ArrayList<>();
		for(int i=0;i<5;i++)
			item11.add((long)i);
		
		Map<String,CooccurrenceCount> counts = p.getCountsAA(item11, store);
		
		// values as in current store
		Assert.assertEquals(item11.size(),counts.size());
		for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			Assert.assertEquals(map.get(e.getKey()), e.getValue());
		
		Map<String,CooccurrenceCount> map2 = new HashMap<>();
		for(int i1 = 0;i1<5;i1++)
			for(int i2=0;i2<5;i2++)
				map2.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(2,0));
		
		store.updateMap(map2);
		
		Thread.sleep(1000);
		
		counts = p.getCountsAA(item11, store);
		
		// as cache has size 1, one value should be as before and the rest should be got from updated store
		Assert.assertEquals(item11.size(),counts.size());
		int eq1Count = 0;
		int eq2Count = 0;
		for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			if (e.getValue().getCount() == 2)
				eq2Count++;
			else if (e.getValue().getCount() == 1)
				eq1Count++;
		
		Assert.assertEquals(4,eq2Count);
		Assert.assertEquals(1,eq1Count);
				
	}
	
	@Test 
	public void testDecay() throws InterruptedException
	{
		CooccurrencePeer p = new CooccurrencePeer("TEST",100,1);
		Map<String,CooccurrenceCount> map = new HashMap<>();
		for(int i1 = 0;i1<5;i1++)
			for(int i2=0;i2<5;i2++)
				map.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(1,0));
		MockCooccurrenceStore store = new MockCooccurrenceStore(map);
		
		List<Long> item11 = new ArrayList<>();
		for(int i=0;i<5;i++)
			item11.add((long)i);
		
		Map<String,CooccurrenceCount> counts = p.getCountsAA(item11, store);
		
		// values as in current store
		Assert.assertEquals(item11.size(),counts.size());
		for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			Assert.assertEquals(map.get(e.getKey()), e.getValue());
		
		Map<String,CooccurrenceCount> map2 = new HashMap<>();
		for(int i1 = 0;i1<5;i1++)
			for(int i2=0;i2<5;i2++)
				map2.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(2,0));
		
		store.updateMap(map2);
		
		Thread.sleep(1000);
		
		counts = p.getCountsAA(item11, store);
		
		// values as in updated store
		Assert.assertEquals(item11.size(),counts.size());
		for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			Assert.assertEquals(map2.get(e.getKey()), e.getValue());
	}
	
	@Test 
	public void testDecay2() throws InterruptedException
	{
		CooccurrencePeer p = new CooccurrencePeer("TEST",100,Long.MAX_VALUE);
		Map<String,CooccurrenceCount> map = new HashMap<>();
		for(int i1 = 0;i1<5;i1++)
			for(int i2=0;i2<5;i2++)
				map.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(1,0));
		MockCooccurrenceStore store = new MockCooccurrenceStore(map);
		
		List<Long> item11 = new ArrayList<>();
		for(int i=0;i<5;i++)
			item11.add((long)i);
		
		Map<String,CooccurrenceCount> counts = p.getCountsAA(item11, store);
		
		// values as in current store
		Assert.assertEquals(item11.size(),counts.size());
		for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			Assert.assertEquals(map.get(e.getKey()), e.getValue());
		
		Map<String,CooccurrenceCount> map2 = new HashMap<>();
		for(int i1 = 0;i1<5;i1++)
			for(int i2=0;i2<5;i2++)
				map2.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(2,0));
		
		store.updateMap(map2);
		
		Thread.sleep(1000);
		
		counts = p.getCountsAA(item11, store);
		
		// values as in original store and thus not from updated store but from cache
		Assert.assertEquals(item11.size(),counts.size());
		for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			Assert.assertEquals(map.get(e.getKey()), e.getValue());
	}
	
	@Test
	public void testCounts()
	{
		CooccurrencePeer p = new CooccurrencePeer("TEST",100,Long.MAX_VALUE);
		Map<String,CooccurrenceCount> map = new HashMap<>();
		Random r = new Random();
		for(int i1 = 0;i1<5;i1++)
			for(int i2=0;i2<5;i2++)
				map.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(r.nextDouble()*100,0));
		
		ICooccurrenceStore store = new MockCooccurrenceStore(map);
		
		List<Long> item11 = new ArrayList<>();
		for(int i=0;i<5;i++)
			item11.add((long)i);
		
		Map<String,CooccurrenceCount> counts = p.getCountsAA(item11, store);
		
		Assert.assertEquals(item11.size(),counts.size());
		for(Map.Entry<String, CooccurrenceCount> e : counts.entrySet())
			Assert.assertEquals(map.get(e.getKey()), e.getValue());
		
		Map<String,CooccurrenceCount> countsAB = p.getCountsAB(item11, item11, store);
		
		Assert.assertEquals(map.size(),countsAB.size());
		for(Map.Entry<String, CooccurrenceCount> e : countsAB.entrySet())
			Assert.assertEquals(map.get(e.getKey()), e.getValue());
		
		//rerun and check the counts are the same - this time will be from cache
		countsAB = p.getCountsAB(item11, item11, store);
		
		Assert.assertEquals(map.size(),countsAB.size());
		for(Map.Entry<String, CooccurrenceCount> e : countsAB.entrySet())
			Assert.assertEquals(map.get(e.getKey()), e.getValue());
	}
	

}
