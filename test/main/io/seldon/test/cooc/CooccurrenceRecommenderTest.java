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

import io.seldon.cooc.CooccurrenceCount;
import io.seldon.cooc.CooccurrencePeer;
import io.seldon.cooc.CooccurrenceRecommender;
import junit.framework.Assert;

import org.junit.Test;

import io.seldon.cooc.ICooccurrenceStore;

public class CooccurrenceRecommenderTest {

	
	@Test 
	public void testSort()
	{
		Map<String,CooccurrenceCount> map = new HashMap<String,CooccurrenceCount>();
		for(int i1 = 0;i1<5;i1++)
			map.put(CooccurrencePeer.getKey(i1, i1), new CooccurrenceCount(50,0));
		
		// items with ids further away have less co-occurrences
		for(int i1 = 0;i1<5;i1++)
			for(int i2=0;i2<5;i2++)
				if (i1 != i2)
					map.put(CooccurrencePeer.getKey(i1, i2), new CooccurrenceCount(50/(Math.abs(i1-i2)+1),0));
		
	ICooccurrenceStore store = new MockCooccurrenceStore(map);
		
	List<Long> itemsToSort = new ArrayList<Long>();
	for(int i=4;i>=1;i--)
		itemsToSort.add((long)i);
		
	List<Long> recentItems = new ArrayList<Long>();
	recentItems.add(0L);
	
	CooccurrenceRecommender r = new CooccurrenceRecommender("TEST", new CooccurrencePeer("TEST",100,Long.MAX_VALUE), store);
	
	List<Long> sorted = r.sort(1L, recentItems, itemsToSort);
	
	Assert.assertEquals(4, sorted.size());
	for(int i=0;i<4;i++)
		Assert.assertEquals(sorted.get(i), new Long(i+1));
	}
}
