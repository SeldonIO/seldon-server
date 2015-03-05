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

package io.seldon.clustering.recommender;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import io.seldon.trust.impl.ItemFilter;
import io.seldon.trust.impl.ItemIncluder;
import io.seldon.memcache.SecurityHashPeer;
import io.seldon.trust.impl.CFAlgorithm;
import net.spy.memcached.MemcachedClient;

import org.junit.Before;
import org.junit.Test;

public class MemcacheAssistedAlgorithmTest {


    public static final ItemRecommendationResultSet RECOMMENDATION_RESULT_SET = new ItemRecommendationResultSet(Collections.EMPTY_LIST);
    private static final int EXP_TIME = 20*60;
    private MemcachedClient mockClient;
    private String client = "example";
    private Long user = 1L;
    private ItemFilter mockFilter;
    private ItemIncluder mockProducer;
    private RecommendationContext mockCtxt;

    @Before
    public void setUp() throws Exception {
        mockClient = createMock(MemcachedClient.class);
        mockProducer = createMock(ItemIncluder.class);
        mockFilter = createMock(ItemFilter.class);
        mockCtxt = createMock(RecommendationContext.class);
    }

    @Test
    public void testThatCachedRecsAreReturnedWhenAvailable() throws Exception {
        MemcacheAlgTestExample alg = new MemcacheAlgTestExample(mockClient);
        List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
        results.add(new ItemRecommendationResultSet.ItemRecommendationResult(1L,1.0f));
        results.add(new ItemRecommendationResultSet.ItemRecommendationResult(2L,1.0f));
        ItemRecommendationResultSet resultsInMemcache = new ItemRecommendationResultSet(results);
        expect(mockClient.get(SecurityHashPeer.md5digest("RecommendedItems:example:MemcacheAlgTestExample:1:0"))).andReturn(resultsInMemcache);
        expect(mockCtxt.getMode()).andReturn(RecommendationContext.MODE.EXCLUSION).times(4);
        expect(mockCtxt.getContextItems()).andReturn(new HashSet<Long>()).times(2);
        replay(mockClient, mockCtxt);

        ItemRecommendationResultSet recs = alg.recommend(client, user, 0, 2,mockCtxt,null);
        verify(mockClient,mockCtxt);
        assertEquals(2,recs.getResults().size());
        assertEquals(resultsInMemcache, recs);
    }

    @Test
    public void testThatFilteringRecsWorks() throws Exception {
        MemcacheAlgTestExample alg = new MemcacheAlgTestExample(mockClient, null, mockFilter);
        List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
        results.add(new ItemRecommendationResultSet.ItemRecommendationResult(1L,1.0f));
        results.add(new ItemRecommendationResultSet.ItemRecommendationResult(2L,1.0f));
        results.add(new ItemRecommendationResultSet.ItemRecommendationResult(3L,1.0f));
        ItemRecommendationResultSet resultsInMemcache = new ItemRecommendationResultSet(results);
        expect(mockClient.get(SecurityHashPeer.md5digest("RecommendedItems:example:MemcacheAlgTestExample:1:0"))).andReturn(resultsInMemcache);
        expect(mockCtxt.getMode()).andReturn(RecommendationContext.MODE.EXCLUSION).times(5);
        expect(mockCtxt.getContextItems()).andReturn(Collections.singleton(3L)).times(3);
        replay(mockClient, mockCtxt);


        ItemRecommendationResultSet recs = alg.recommend(client, user, 0, 2,mockCtxt, null);
        verify(mockClient, mockCtxt);
        assertEquals(2,recs.getResults().size());

        List<ItemRecommendationResultSet.ItemRecommendationResult> newResults = resultsInMemcache.getResults();
        newResults.remove(2);
        resultsInMemcache = new ItemRecommendationResultSet(newResults);
        assertEquals(resultsInMemcache, recs);
    }

    @Test
    public void testThatCachedRecsAreNotReturnedWhenNothingCached() {
        MemcacheAlgTestExample alg = new MemcacheAlgTestExample(mockClient);
        expect(mockClient.get(SecurityHashPeer.md5digest("RecommendedItems:example:MemcacheAlgTestExample:1:0"))).andReturn(null);
        expect(mockClient.set(SecurityHashPeer.md5digest("RecommendedItems:example:MemcacheAlgTestExample:1:0"), EXP_TIME, RECOMMENDATION_RESULT_SET)).andReturn(null);
        replay(mockClient);

        alg.recommend(client, user, 0, 2,null,null);
        verify(mockClient);
        assertTrue(alg.recWithoutCacheCalled);
    }

    @Test
    public void testThatExceptionHandlingWorks() {
        MemcacheAlgTestExample alg = new MemcacheAlgTestExample(mockClient);
        expect(mockClient.get(SecurityHashPeer.md5digest("RecommendedItems:example:MemcacheAlgTestExample:1:0"))).andThrow(new RuntimeException());
        expect(mockClient.set(SecurityHashPeer.md5digest("RecommendedItems:example:MemcacheAlgTestExample:1:0"), EXP_TIME, RECOMMENDATION_RESULT_SET)).andReturn(null);
        replay(mockClient);

        alg.recommend(client, user, 0, 2,null,null);
        verify(mockClient);
        assertTrue(alg.recWithoutCacheCalled);
    }

    @Test
    public void testThatCachedRecAreNotReturnedWhenThereArentEnough() {
        MemcacheAlgTestExample alg = new MemcacheAlgTestExample(mockClient);
        List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
        results.add(new ItemRecommendationResultSet.ItemRecommendationResult(1L,1.0f));
        ItemRecommendationResultSet resultsInMemcache = new ItemRecommendationResultSet(results);
        expect(mockClient.get(SecurityHashPeer.md5digest("RecommendedItems:example:MemcacheAlgTestExample:1:0"))).andReturn(resultsInMemcache);
        expect(mockClient.set(SecurityHashPeer.md5digest("RecommendedItems:example:MemcacheAlgTestExample:1:0"), EXP_TIME, RECOMMENDATION_RESULT_SET)).andReturn(null);
        expect(mockCtxt.getMode()).andReturn(RecommendationContext.MODE.EXCLUSION).times(2);
        expect(mockCtxt.getContextItems()).andReturn(new HashSet<Long>());
        replay(mockClient, mockCtxt);

        alg.recommend(client, user,0, 2,mockCtxt,null);
        verify(mockClient,mockCtxt);
        assertTrue(alg.recWithoutCacheCalled);
    }

    private class MemcacheAlgTestExample extends MemcachedAssistedAlgorithm {

        public MemcachedClient mockClient;
        public boolean recWithoutCacheCalled = false;

        public MemcacheAlgTestExample(MemcachedClient mockClient) {
            this.mockClient = mockClient;
        }


        public MemcacheAlgTestExample(MemcachedClient mockClient, ItemIncluder producer, ItemFilter filter) {

            this.mockClient = mockClient;
        }

        @Override
        public ItemRecommendationResultSet recommendWithoutCache(String client, Long user, int dimension,RecommendationContext ctxt, int maxRecsCount, List<Long> recentItems) {
            recWithoutCacheCalled = true;
            return RECOMMENDATION_RESULT_SET;
        }

        @Override
        public MemcachedClient getMemcache(){
            return mockClient;
        }

        @Override
        public String name() {
            return "TEST";
        }
    }
}