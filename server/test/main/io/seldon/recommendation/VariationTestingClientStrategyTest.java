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
package io.seldon.recommendation;

import io.seldon.recommendation.VariationTestingClientStrategy.Variation;
import io.seldon.recommendation.combiner.AlgorithmResultsCombiner;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.lang.math.NumberRange;
import org.junit.Test;

public class VariationTestingClientStrategyTest {

	public static class TestStrategy implements ClientStrategy {

		String name;
		
		public TestStrategy(String name) {
			super();
			this.name = name;
		}

		@Override
		public Double getDiversityLevel(String userId, String recTag) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public List<AlgorithmStrategy> getAlgorithms(String userId,
				String recTag) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public AlgorithmResultsCombiner getAlgorithmResultsCombiner(
				String userId, String recTag) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getName(String userId, String recTag) {
			return name;
		}

		@Override
		public Map<Integer, Double> getActionsWeights(String userId,
				String recTag) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	@Test
	public void testSampling()
	{
		List<Variation> variations = new ArrayList<Variation>();
		int max = 4;
		double ratio = 1/(max*1.0);
		for (int i=1;i<=max;i++)
		{
			ClientStrategy s = new TestStrategy(""+i);
			Variation v = new Variation(s,new BigDecimal(ratio));
			variations.add(v);
		}
		VariationTestingClientStrategy v = VariationTestingClientStrategy.build(variations);
		Random r = new Random();
		int[] sTot = new int[max];
		int sampleSize = 10000000;
		for(int i=0;i<sampleSize;i++)
		{
			String userId = "u"+r.nextLong();
			ClientStrategy s =  v.sample(userId);
			Assert.assertNotNull(s);
			int index = Integer.parseInt(s.getName(null, null)) - 1;
			sTot[index] += 1;
		}
		Assert.assertEquals(1/(max*1.0), sTot[0]/(sampleSize*1.0), 0.001);
	}
	
	@Test
	public void rangeTest()
	{
		BigDecimal ratioTotal = new BigDecimal(1.0);
		BigDecimal currentMax = new BigDecimal(0.0);
		BigDecimal r1 = new BigDecimal(0.9);
		BigDecimal r2 = new BigDecimal(0.1);
		NumberRange range1 = new NumberRange(currentMax, currentMax.add(r1.divide(ratioTotal, 5, BigDecimal.ROUND_UP)));
		currentMax = currentMax.add(r1);
		NumberRange range2 = new NumberRange(currentMax.add(new BigDecimal(0.0001)), currentMax.add(r2.divide(ratioTotal, 5, BigDecimal.ROUND_UP)));
		BigDecimal t = new BigDecimal(0.900001);
		Assert.assertTrue(range1.containsNumber(t));
		Assert.assertFalse(range2.containsNumber(t));
		BigDecimal t2 = new BigDecimal(0.901);
		Assert.assertFalse(range1.containsNumber(t2));
		Assert.assertTrue(range2.containsNumber(t2));
		
	}
	
}
