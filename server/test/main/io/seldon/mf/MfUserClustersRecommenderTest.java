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
package io.seldon.mf;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import io.seldon.api.resource.service.ItemService;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.mf.MfUserClustersModelManager.MfUserModel;
import io.seldon.recommendation.Recommendation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class MfUserClustersRecommenderTest {

	private RecommendationContext mockCtxt;
	private RecommendationContext.OptionsHolder mockOptions;
	private MfUserClustersModelManager mockModelManager;
	private ItemService mockItemService;
	
	@Before
	public void createMocks()
	{
		mockModelManager = createMock(MfUserClustersModelManager.class);
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
		
		Map<Long,Integer> userClusters = new HashMap<Long,Integer>();
		userClusters.put(1L, 1);
		List<Recommendation> recs = new ArrayList<Recommendation>();
		recs.add(new Recommendation(1L, 1, 1.0));
		recs.add(new Recommendation(2L, 1, 0.9));
		recs.add(new Recommendation(3L, 1, 0.8));
		Map<Integer,List<Recommendation>> recMap = new HashMap<>();
		recMap.put(1, recs);
		MfUserClustersModelManager.MfUserModel model = new MfUserModel(userClusters, recMap);
		
		expect(mockModelManager.getClientStore(client, mockOptions)).andReturn(model);
		replay(mockModelManager);
		expect(mockCtxt.getOptsHolder()).andReturn(mockOptions);
		expect(mockCtxt.getMode()).andReturn(RecommendationContext.MODE.EXCLUSION);
		expect(mockCtxt.getContextItems()).andReturn(Collections.singleton(1L));

		replay(mockCtxt, mockOptions);
		MfUserClustersRecommender r = new MfUserClustersRecommender(mockModelManager);
		ItemRecommendationResultSet res = r.recommend(client, 1L, dimensions,50,mockCtxt,null);
		
		
		verify(mockModelManager,mockCtxt, mockOptions);
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
		
		expect(mockModelManager.getClientStore(client, mockOptions)).andReturn(null);
		replay(mockModelManager);
		expect(mockCtxt.getOptsHolder()).andReturn(mockOptions);

		replay(mockCtxt, mockOptions);
		MfUserClustersRecommender r = new MfUserClustersRecommender(mockModelManager);
		ItemRecommendationResultSet res = r.recommend(client, 1L, dimensions,50,mockCtxt,null);
		
		
		verify(mockModelManager,mockCtxt, mockOptions);
		Assert.assertNotNull(res);
		Assert.assertNotNull(res.getResults());
		Assert.assertEquals(0, res.getResults().size());
	}
	
}
