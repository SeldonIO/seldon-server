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

package io.seldon.test.trust.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import junit.framework.Assert;

import org.junit.Test;

import io.seldon.trust.impl.jdo.RecommendationUtils;



public class TestRecommendationUtils extends BasePeerTest {

	private void clearMemcache(String client,String userId,int dimension)
	{
		MemCachePeer.delete(MemCacheKeys.getExcludedItemsForRecommendations(client, userId));
		MemCachePeer.delete(MemCacheKeys.getRecentRecsForUser(client, userId, dimension));
		MemCachePeer.delete(MemCacheKeys.getRecommendationListUserCounter(client, dimension, userId));
	}
	
	String getSignature(List<Long> recs)
	{
		return ""+recs.hashCode();
	}
	
	
	@Test
	public void testNormalizeScores()
	{
		Map<Long,Double> scores1 = new HashMap<>();
		scores1.put(1L, 0.1);
		scores1.put(2L, 0.1);
		scores1.put(3L, 0.05);
		
		Map<Long,Double> scores2 = RecommendationUtils.normaliseScores(scores1, 3);
		
		for(Map.Entry<Long, Double> score : scores2.entrySet())
			System.out.println(""+score.getKey()+"->"+score.getValue());
		
		Assert.assertEquals(0.4, scores2.get(1L));
		Assert.assertEquals(0.4, scores2.get(2L));
		Assert.assertEquals(0.2, scores2.get(3L));
		
	}
	
	@Test
	public void testRescaleScores()
	{
		Map<Long,Double> scores1 = new HashMap<>();
		scores1.put(1L, 0.1);
		scores1.put(2L, 0.1);
		scores1.put(3L, 0.05);
		
		Map<Long,Double> scores2 = RecommendationUtils.rescaleScoresToOne(scores1, 3);
		
		for(Map.Entry<Long, Double> score : scores2.entrySet())
			System.out.println(""+score.getKey()+"->"+score.getValue());
		
		
		Assert.assertEquals(1.0, scores2.get(1L));
		Assert.assertEquals(1.0, scores2.get(2L));
		Assert.assertEquals(0.5, scores2.get(3L));
	}
	
	@Test
	public void testItemAddedWhenFirst()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		ArrayList<Long> recs = new ArrayList<>();
		recs.add(1L); recs.add(2L); recs.add(3L);
		CFAlgorithm alg = new CFAlgorithm();
		alg.setRemoveIgnoredRecommendations(true);
		List<Long> recList = new ArrayList<>();
		for(Long r : recs)
			recList.add(r);
		String uuid = RecommendationUtils.cacheRecommendationsAndCreateNewUUID(client, userId,0,null, recList, alg,"",1L,0);
		Set<Long> excl = RecommendationUtils.getExclusions(true,client, userId, 1L,uuid, alg,0);
		Assert.assertEquals(1, excl.size());
		Assert.assertTrue(excl.contains(1L));
		
	}
	
	@Test 
	public void checkUUIDIncrementedWhenGaps()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		ArrayList<Long> recs = new ArrayList<>();
		recs.add(1L); recs.add(2L); recs.add(3L);
		CFAlgorithm  alg = new CFAlgorithm();
		alg.setRemoveIgnoredRecommendations(true);
		List<Long> recList = new ArrayList<>();
		for(Long r : recs)
			recList.add(r);
		String uuid = RecommendationUtils.cacheRecommendationsAndCreateNewUUID(client, userId,0,"1", recList, alg,"",1L,0);
		uuid = RecommendationUtils.cacheRecommendationsAndCreateNewUUID(client, userId,0,null, recList, alg,"",1L,0);
		uuid = RecommendationUtils.cacheRecommendationsAndCreateNewUUID(client, userId,0,"1", recList, alg,"",1L,0);
		Assert.assertEquals("3", uuid);
	}
	
	@Test
	public void testDiversityNotDone()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		Random rand = new Random();
		List<Long> recs = new ArrayList<>();
		for(int i=0;i<10;i++)
			recs.add(rand.nextLong());
		List<Long> recsFinal = RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		Assert.assertEquals(5, recsFinal.size());
		Assert.assertEquals(getSignature(recs.subList(0, 5)), getSignature(recsFinal)); //should be same as user has not yet had any recs
	}
	
	@Test
	public void testDiversity()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		Random rand = new Random();
		List<Long> recs = new ArrayList<>();
		for(int i=0;i<10;i++)
			recs.add(rand.nextLong());
		List<Long> recsFinal = RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		Assert.assertEquals(5, recsFinal.size());
		Assert.assertTrue(getSignature(recs.subList(0, 5)).equals(getSignature(recsFinal))); 
		recsFinal = RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		Assert.assertEquals(5, recsFinal.size());
		Assert.assertFalse(getSignature(recs.subList(0, 5)).equals(getSignature(recsFinal))); 
	}
	
	@Test
	public void testDiversityMultipleRecs()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		Random rand = new Random();
		List<Long> recs = new ArrayList<>();
		for(int i=0;i<50;i++)//set to 50 as its not guaranteed will be different just unlikely
			recs.add(rand.nextLong());
		List<Long> recsFinal1 = RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		Assert.assertEquals(5, recsFinal1.size());
		Assert.assertTrue(getSignature(recs.subList(0, 5)).equals(getSignature(recsFinal1))); 
		List<Long>  recsFinal2 = RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		Assert.assertEquals(5, recsFinal2.size());
		Assert.assertFalse(getSignature(recs.subList(0, 5)).equals(getSignature(recsFinal2))); 
		List<Long>  recsFinal3 = RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		Assert.assertEquals(5, recsFinal3.size());
		Assert.assertFalse(getSignature(recs.subList(0, 5)).equals(getSignature(recsFinal3))); 
		Assert.assertFalse(getSignature(recsFinal2.subList(0, 5)).equals(getSignature(recsFinal3))); 
	}
	
	@Test
	public void testDiversityWhenFewRecsReturned1()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		Random rand = new Random();
		List<Long> recs = new ArrayList<>();
		for(int i=0;i<3;i++)
			recs.add(rand.nextLong());

		List<Long> recsFinal = RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		Assert.assertEquals(3, recsFinal.size());
		Assert.assertEquals(getSignature(recs.subList(0, 3)), getSignature(recsFinal)); //should be same as user has not yet had any recs
	}
	
	@Test
	public void testDiversityWhenFewRecsReturned2()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		Random rand = new Random();
		List<Long> recs = new ArrayList<>();
		for(int i=0;i<3;i++)
			recs.add(rand.nextLong());
		List<Long> recsFinal = RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		Assert.assertEquals(3, recsFinal.size()); 
	}
	
	
	@Test
	public void testDiversitySpeed()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		Random rand = new Random();
		List<Long> recs = new ArrayList<>();
		for(int i=0;i<10;i++)
			recs.add(rand.nextLong());
		long start = System.currentTimeMillis();
		for(int i=0;i<1000;i++)
			RecommendationUtils.getDiverseRecommendations(5, recs,client,userId,dimension);
		long end = System.currentTimeMillis();
		System.out.println("1000 diversity runs took "+(end-start));
		
	}
		
	@Test
	public void testItemAdded()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		ArrayList<Long> recs = new ArrayList<>();
		recs.add(1L); recs.add(2L); recs.add(3L);
		CFAlgorithm  alg = new CFAlgorithm();
		alg.setRemoveIgnoredRecommendations(true);
		List<Long> recList = new ArrayList<>();
		for(Long r : recs)
			recList.add(r);
		String uuid = RecommendationUtils.cacheRecommendationsAndCreateNewUUID(client, userId,0,null, recList, alg,"",1L,0);
		Set<Long> excl = RecommendationUtils.getExclusions(true,client, userId, 2L,uuid, alg,0);
		Assert.assertEquals(2, excl.size());
		Assert.assertTrue(excl.contains(1L));
		Assert.assertTrue(excl.contains(2L));
	}
	
	
	
	@Test 
	public void checkUUIDIncremented()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		ArrayList<Long> recs = new ArrayList<>();
		recs.add(1L); recs.add(2L); recs.add(3L);
		CFAlgorithm  alg = new CFAlgorithm();
		alg.setRemoveIgnoredRecommendations(true);
		List<Long> recList = new ArrayList<>();
		for(Long r : recs)
			recList.add(r);
		String uuid = RecommendationUtils.cacheRecommendationsAndCreateNewUUID(client, userId,0,"1", recList, alg,"",1L,0);
		Assert.assertEquals("1", uuid);
	}
	
	@Test
	public void testInactive() //so no exlusions done
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		ArrayList<Long> recs = new ArrayList<>();
		recs.add(1L); recs.add(2L); recs.add(3L);
		CFAlgorithm  alg = new CFAlgorithm();
		alg.setRemoveIgnoredRecommendations(false);
		List<Long> recList = new ArrayList<>();
		for(Long r : recs)
			recList.add(r);
		String uuid = RecommendationUtils.cacheRecommendationsAndCreateNewUUID(client, userId, 0,"", recList, alg,"",1L,0);
		Set<Long> excl = RecommendationUtils.getExclusions(true,client, userId,2L,uuid, alg,0);
		Assert.assertEquals(0, excl.size());
	}
	
	@Test
	public void testInactiveWithNullUUID()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		ArrayList<Long> recs = new ArrayList<>();
		recs.add(1L); recs.add(2L); recs.add(3L);
		CFAlgorithm  alg = new CFAlgorithm();
		alg.setRemoveIgnoredRecommendations(false);

		Set<Long> excl = RecommendationUtils.getExclusions(true,client, userId, 2L,null, alg,0);
		Assert.assertEquals(0, excl.size());
	}
	
	@Test
	public void testInactiveWithNullItem()
	{
		String client = "test";
		String userId = ""+1L;
		int dimension = 0;
		clearMemcache(client, userId, dimension);
		ArrayList<Long> recs = new ArrayList<>();
		recs.add(1L); recs.add(2L); recs.add(3L);
		CFAlgorithm  alg = new CFAlgorithm();
		alg.setRemoveIgnoredRecommendations(false);

		Set<Long> excl = RecommendationUtils.getExclusions(true,client, userId,null,"1234", alg,0);
		Assert.assertEquals(0, excl.size());
	}


}
