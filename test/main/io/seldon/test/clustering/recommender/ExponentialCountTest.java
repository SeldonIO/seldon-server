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

import io.seldon.clustering.recommender.ExponentialCount;
import junit.framework.Assert;

import org.junit.Test;

public class ExponentialCountTest {

	@Test
	public void simpleTest()
	{
		// count put in doesn't change
		ExponentialCount c = new ExponentialCount(1,1,1);
		Assert.assertEquals(1D,c.get(1));
	}
	
	@Test 
	public void decayTest()
	{
		//exponential decay
		ExponentialCount c = new ExponentialCount(1,1,1);
		c.increment(1, 2);
		Assert.assertEquals(1.36,c.get(2),0.1);	
		
		//exponential decay with get coming later
		c = new ExponentialCount(1,1,1);
		c.increment(1, 2);
		Assert.assertEquals(0.50,c.get(3),0.1);	
		
	}
	
}
