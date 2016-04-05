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
package io.seldon.recommendation.baseline;

import io.seldon.clustering.recommender.ItemRecommendationResultSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

public class RecentInteractionsRecommenderTest {

	@Test
	public void testSimple()
	{
		final String client = "test";
		final int dimension = 1;
		Set<Integer> dimensions = new HashSet<Integer>();
		dimensions.add(dimension);
		
		List<Long> recentItemInteractions = new ArrayList<Long>();
		Long item1 = 1L;
		Long item2 = 2L;
		recentItemInteractions.add(item1);
		recentItemInteractions.add(item2);
		RecentInteractionsRecommender r = new RecentInteractionsRecommender();
		ItemRecommendationResultSet res = r.recommend(client, null, dimensions, 2, null, recentItemInteractions);
		Assert.assertEquals(2,res.getResults().size());
		Assert.assertEquals(item1,res.getResults().get(0).item);
		Assert.assertEquals(1.0f,res.getResults().get(0).score);
		Assert.assertEquals(item2,res.getResults().get(1).item);
		Assert.assertEquals(0.5f,res.getResults().get(1).score);
		
	}
	
	@Test
	public void testMaxRecs()
	{
		final String client = "test";
		final int dimension = 1;
		Set<Integer> dimensions = new HashSet<Integer>();
		dimensions.add(dimension);
		
		List<Long> recentItemInteractions = new ArrayList<Long>();
		Long item1 = 1L;
		Long item2 = 2L;
		Long item3 = 3L;
		recentItemInteractions.add(item1);
		recentItemInteractions.add(item2);
		recentItemInteractions.add(item3);
		RecentInteractionsRecommender r = new RecentInteractionsRecommender();
		ItemRecommendationResultSet res = r.recommend(client, null, dimensions, 2, null, recentItemInteractions);
		Assert.assertEquals(2,res.getResults().size());
		Assert.assertEquals(item1,res.getResults().get(0).item);
		Assert.assertEquals(1.0f,res.getResults().get(0).score);
		Assert.assertEquals(item2,res.getResults().get(1).item);
		Assert.assertEquals(0.666f,res.getResults().get(1).score,0.01);
		
	}
	
}
