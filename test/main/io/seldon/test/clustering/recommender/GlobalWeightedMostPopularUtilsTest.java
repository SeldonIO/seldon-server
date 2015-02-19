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
import java.util.Random;

import io.seldon.clustering.recommender.GlobalWeightedMostPopularUtils;
import io.seldon.clustering.recommender.MemoryWeightedClusterCountMap;
import io.seldon.trust.impl.ItemsRankingManager;
import junit.framework.Assert;

import org.junit.Test;

public class GlobalWeightedMostPopularUtilsTest {

	@Test
	public void sortTestSameTime()
	{
		MemoryWeightedClusterCountMap m = new MemoryWeightedClusterCountMap(1000, 10);
		m.incrementCount(1L, 1, 1);
		m.incrementCount(1L, 1, 1);
		m.incrementCount(1L, 1, 1);
		m.incrementCount(2L, 1, 1);
		m.incrementCount(2L, 1, 1);
		m.incrementCount(3L, 1, 1);
		
		GlobalWeightedMostPopularUtils utils = new GlobalWeightedMostPopularUtils(m, 1);
		
		List<Long> items = new ArrayList<>();

		items.add(3L);
		items.add(2L);
		items.add(1L);
		
		List<Long> sorted = utils.sort(items);
		Assert.assertEquals((Long)1L, sorted.get(0));
		Assert.assertEquals((Long)2L, sorted.get(1));
		Assert.assertEquals((Long)3L, sorted.get(2));
	}
	
	@Test
	public void mergeTestSameTime()
	{
		MemoryWeightedClusterCountMap m = new MemoryWeightedClusterCountMap(1000, 10);
		m.incrementCount(1L, 1, 1);
		m.incrementCount(1L, 1, 1);
		m.incrementCount(1L, 1, 1);
		m.incrementCount(2L, 1, 1);
		m.incrementCount(2L, 1, 1);
		m.incrementCount(3L, 1, 1);
		
		GlobalWeightedMostPopularUtils utils = new GlobalWeightedMostPopularUtils(m, 1);
		
		List<Long> items = new ArrayList<>();


		items.add(2L);
		items.add(3L);
		items.add(1L);		

		// item order passed in should dominate
		List<Long> sorted = utils.merge(items,0.9f);
		Assert.assertEquals((Long)2L, sorted.get(0));
		Assert.assertEquals((Long)3L, sorted.get(1));
		Assert.assertEquals((Long)1L, sorted.get(2));
		
		// sort order should dominate
		sorted = utils.merge(items,0.1f);
		Assert.assertEquals((Long)1L, sorted.get(0));
		Assert.assertEquals((Long)2L, sorted.get(1));
		Assert.assertEquals((Long)3L, sorted.get(2));
	}
	
	@Test 
	public void compareToMostPopular()
	{
		MemoryWeightedClusterCountMap m = new MemoryWeightedClusterCountMap(1000, 1000000000);
		Random r = new Random();
		for(int i=0;i<10000;i++)
		{
			long item = r.nextInt(100);
			m.incrementCount(item, 1, i);
			ItemsRankingManager.getInstance().Hit("test", item);
		}
		
		List<Long> items = new ArrayList<>();
		for(long i=0;i<100;i++)
			items.add(i);
		
		GlobalWeightedMostPopularUtils u = new GlobalWeightedMostPopularUtils(m,1001);
		List<Long> sorted1 = u.sort(items);
		List<Long> sorted2 = ItemsRankingManager.getInstance().getItemsByHits("test", items);
		Assert.assertEquals(sorted1.size(), sorted2.size());
		for(int i=0;i<sorted1.size();i++)
		{
			System.out.println("Pos "+i+ " "+sorted1.get(i)+" "+sorted2.get(i));
		}
	}
	
}
