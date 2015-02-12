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

import io.seldon.clustering.recommender.MemcacheClusterCountDecayStore;
import io.seldon.test.peer.BasePeerTest;
import org.junit.Assert;
import org.junit.Test;

import io.seldon.clustering.recommender.ExponentialCount;

public class MemcacheClusterCountDecayStoreTest extends BasePeerTest {

	@Test
	public void noDecayTest()
	{
		MemcacheClusterCountDecayStore store = new MemcacheClusterCountDecayStore("test", 0, 0, 999999999);
		store.clear(1, 1, 1);
		store.add(1, 1, 1, 1, 1);
		store.add(1, 1, 1, 1, 1);
		double val = store.getCount(1,1,1,1);
		Assert.assertEquals(2, val,0.01);
	}
	
	@Test 
	public void decayTest()
	{
		MemcacheClusterCountDecayStore store = new MemcacheClusterCountDecayStore("test",0, 0, 1);
		store.clear(1, 1, 1);
		ExponentialCount c = new ExponentialCount(1, 0, 1);
		store.add(1, 1, 1, 1, 2);
		c.increment(1, 2);
		store.add(1, 1, 1, 1, 3);
		c.increment(1, 3);
		double val = store.getCount(1,1,1,3);
		double correct = c.get(3);
		System.out.println("Decayed value is "+val);
		System.out.println("Correct value is "+correct);
		Assert.assertEquals(correct, val,0.01);
	}
	
}
