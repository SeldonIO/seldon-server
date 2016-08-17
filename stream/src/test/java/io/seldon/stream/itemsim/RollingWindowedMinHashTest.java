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

import io.seldon.stream.itemsim.minhash.Hasher;
import io.seldon.stream.itemsim.minhash.SimplePrimeHash;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class RollingWindowedMinHashTest {

	@Test
	public void testDecreasingMinHash()
	{
		Hasher h = new SimplePrimeHash(1,2);
		RollingWindowedMinHash mh = new RollingWindowedMinHash(h, 10);
		for(int i=10;i>=1;i--)
		{
			mh.add(i, 11-i);
		}
		Assert.assertEquals(1, mh.hashes.size());
		Assert.assertEquals(10, mh.getCount(10));
	}
	
	@Test
	public void testIncreasingMinHash()
	{
		Hasher h = new SimplePrimeHash(1,2);
		RollingWindowedMinHash mh = new RollingWindowedMinHash(h, 10);
		for(int i=0;i<=10;i++)
		{
			mh.add(i, i);
		}
		Assert.assertEquals(10, mh.hashes.size());
		Assert.assertEquals(10, mh.getCount(10));
	}
	
	@Test
	public void sizeTest()
	{
		Hasher h = new SimplePrimeHash(1,2);
		RollingWindowedMinHash mh = new RollingWindowedMinHash(h, 10000);
		Random r = new Random();
		int timeSteps = 100000;
		for(int i=1;i<timeSteps;i++)
		{
			int id = r.nextInt(2000);
			mh.add(id, i);
		}
		System.out.println(mh.hashes.size());
	}
	
}
