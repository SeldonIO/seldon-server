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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.recommendation.baseline.MostPopularInSessionFeaturesManager.DimPopularityStore;
import io.seldon.recommendation.baseline.MostPopularInSessionFeaturesManager.ItemCount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class MostPopularInSessionRecommenderTest {
	
	private RecommendationContext mockCtxt;
	private RecommendationContext.OptionsHolder mockOptions;
	private MostPopularInSessionFeaturesManager mockFeaturesManager;
	private ItemService mockItemService;
	
	@Before
	public void createMocks()
	{
		mockFeaturesManager = createMock(MostPopularInSessionFeaturesManager.class);
		mockCtxt = createMock(RecommendationContext.class);
		mockOptions = createMock(RecommendationContext.OptionsHolder.class);
		mockItemService = createMock(ItemService.class);
	}
	
	@Test
	public void testRecommendations()
	{
		final String client = "test";
		final int dimension = 1;
		Set<Integer> dimensions = new HashSet<Integer>();
		dimensions.add(dimension);

		expect(mockCtxt.getOptsHolder()).andReturn(mockOptions);
		expect(mockCtxt.getMode()).andReturn(RecommendationContext.MODE.EXCLUSION);
		expect(mockCtxt.getContextItems()).andReturn(Collections.singleton(1L));
		expect(mockOptions.getStringOption("io.seldon.algorithm.popular.attrs")).andReturn("attr1");
		expect(mockOptions.getIntegerOption("io.seldon.algorithm.popular.recent.depth")).andReturn(1);
		replay(mockCtxt, mockOptions);
		
		Map<String,Integer> attrDims = new HashMap<>();
		attrDims.put("attr1", 1);
		expect(mockItemService.getDimensionIdsForItem((ConsumerBean)EasyMock.anyObject(),EasyMock.anyInt())).andReturn(attrDims);
		replay(mockItemService);
		
		Map<Integer, List<ItemCount>> dimToPopularItems = new HashMap<>();
		List<ItemCount> l1 = new ArrayList<>();
		ItemCount i1 = new ItemCount(22,10.0f);
		ItemCount i2 = new ItemCount(21,9.0f);
		ItemCount i3 = new ItemCount(20,8.0f);
		l1.add(i1);l1.add(i2);l1.add(i3);
		dimToPopularItems.put(1, l1);
		DimPopularityStore dps = new DimPopularityStore(dimToPopularItems);
		expect(mockFeaturesManager.getClientStore(client, mockOptions)).andReturn(dps);
		replay(mockFeaturesManager);
		
		List<Long> recentItems = new ArrayList<>();
		recentItems.add(1L);
		MostPopularInSessionRecommender r = new MostPopularInSessionRecommender(mockFeaturesManager,mockItemService);
		ItemRecommendationResultSet res = r.recommend(client, 1L, dimensions,2,mockCtxt,recentItems);
		
		verify(mockFeaturesManager,mockCtxt, mockOptions);
		Assert.assertNotNull(res);
		Assert.assertNotNull(res.getResults());
		Assert.assertEquals(2, res.getResults().size());
	}
	
	@Test
	public void testNoFeatures()
	{
		final String client = "test";
		final int dimension = 1;
		Set<Integer> dimensions = new HashSet<Integer>();
		dimensions.add(dimension);
		expect(mockFeaturesManager.getClientStore(client, mockOptions)).andReturn(null);
		replay(mockFeaturesManager);
		expect(mockCtxt.getOptsHolder()).andReturn(mockOptions);

		replay(mockCtxt, mockOptions);
		MostPopularInSessionRecommender r = new MostPopularInSessionRecommender(mockFeaturesManager,null);
		ItemRecommendationResultSet res = r.recommend(client, 1L, dimensions,50,mockCtxt,null);
		
		
		verify(mockFeaturesManager,mockCtxt, mockOptions);
		Assert.assertNotNull(res);
		Assert.assertNotNull(res.getResults());
		Assert.assertEquals(0, res.getResults().size());
	}
}
