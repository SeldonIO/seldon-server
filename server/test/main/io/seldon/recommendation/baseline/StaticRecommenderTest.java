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
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.ExceptionSwallowingMemcachedClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class StaticRecommenderTest {

	private IStaticRecommendationsProvider mockStaticRecs;
	private ExceptionSwallowingMemcachedClient mockMemcacheClient;
	private DogpileHandler dogPileHandler;
	@Before
	public void createMocks()
	{
		mockStaticRecs = createMock(IStaticRecommendationsProvider.class);
		mockMemcacheClient = createMock(ExceptionSwallowingMemcachedClient.class);
		dogPileHandler = new DogpileHandler();
	}
	
	@Test
	public void emptyResultsTest()
	{
		Map<Long,Double> scores = new HashMap<Long,Double>();
		expect(mockStaticRecs.getStaticRecommendations(EasyMock.anyLong(), (Set<Integer>) EasyMock.anyObject(), EasyMock.anyInt())).andReturn(scores).anyTimes();
		replay(mockStaticRecs);
		expect(mockMemcacheClient.get((String) EasyMock.anyObject())).andReturn(null).once();
		expect(mockMemcacheClient.set((String) EasyMock.anyObject(),EasyMock.anyInt(),EasyMock.anyObject())).andReturn(null).once();
		replay(mockMemcacheClient);
		Set<Integer> dims = new HashSet<>();
		dims.add(1);
		StaticRecommender r = new StaticRecommender(mockMemcacheClient);
		List<Long> recentItemInteractions = new ArrayList<>();
		ItemRecommendationResultSet res = r.recommend("test", 1L, dims, 10, null, recentItemInteractions);
		Assert.assertEquals(0, res.getResults().size());
	}
	
	
}
