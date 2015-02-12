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

package io.seldon.test.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.seldon.util.CollectionTools;
import junit.framework.Assert;

import org.junit.Test;

import io.seldon.trust.impl.RankedItem;

public class CollectionToolsTest {

	@Test
	public void rankedSortTest()
	{
		Map<Integer,Integer> map = new HashMap<Integer,Integer>();
		map.put(1,10);
		map.put(2,10);
		map.put(3, 9);
		map.put(4, 5);
		
		List<RankedItem<Integer>> results = CollectionTools.sortMapAndLimitToRankedList(map, map.size(), true);
		int pos = 1;
		for(RankedItem<Integer> r : results)
		{
			if (pos == 1 || pos > 2)
				Assert.assertEquals(pos, (int)r.getPos());
			else if (pos == 2)
				Assert.assertEquals(1, (int)r.getPos());

			System.out.println(""+(pos++)+": id:"+r.getItemId()+" rank:"+r.getPos());
		}
	}
	
}
