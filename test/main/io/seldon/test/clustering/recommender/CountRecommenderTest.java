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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jdo.PersistenceManager;

import io.seldon.api.Util;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.UserCluster;
import io.seldon.clustering.recommender.UserClusterStore;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.AlgorithmServiceImpl;
import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.clustering.recommender.ClusterCountStore;
import io.seldon.util.CollectionTools;

public class CountRecommenderTest extends BasePeerTest {
	
	private static final double LONG_DECAY = 100000;
	private static final String decayClient = "decayClient";
	
	@Autowired PersistenceManager pm;
	
	@Autowired
	GenericPropertyHolder props;
	
	private void createClusterCountFactory()
	{
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient()+","+decayClient);
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
	
		clusterProps.put("io.seldon.memoryclusters."+decayClient+".type", "DECAY");
		clusterProps.put("io.seldon.memoryclusters."+decayClient+".threshold", "0");
		clusterProps.put("io.seldon.memoryclusters."+decayClient+".decay", "1");
	
		MemoryClusterCountFactory.create(clusterProps);
	}
	
	@Before
	public void setup()
	{
		this.createClusterCountFactory();
		Properties testingProps = new Properties();
		testingProps.put("io.seldon.testing", "true");
		TestingUtils.initialise(testingProps);
	}
	
	@After 
	public void tearDown()
	{
		MemoryClusterCountFactory.create(new Properties());
		Properties testingProps = new Properties();
		testingProps.put("io.seldon.testing", "false");
		TestingUtils.initialise(testingProps);
	}
	
	@Test
	public void testBucketClusters()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new EmptyUserClusters();
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<>();
        CFAlgorithm alg = new CFAlgorithm();
        alg.setUseBucketCluster(true);
        map.put(props.getClient(), alg);
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		cr.addCount(1L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
		
		Map<Long,Double> rMap =  cr.recommendGlobal(Constants.DEFAULT_DIMENSION, 1,new HashSet<Long>(),LONG_DECAY,null);
		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// item 1 has most counts
		Assert.assertEquals(new Long(1), (Long)recs.iterator().next());
	}
	
	@Test
	public void testBucketClustersNotActive()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new EmptyUserClusters();
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<>();
        map.put(props.getClient(), new CFAlgorithm());
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		cr.addCount(1L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
		
		Map<Long,Double> rMap =  cr.recommendGlobal(Constants.DEFAULT_DIMENSION, 1,new HashSet<Long>(),LONG_DECAY,null);
		Assert.assertEquals(0, rMap.size());
	}
	
	@Test
	public void testRecommendGlobalSimple()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		cr.addCount(1L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
		
		Map<Long,Double> rMap =  cr.recommendGlobal(Constants.DEFAULT_DIMENSION, 1,new HashSet<Long>(),LONG_DECAY,null);
		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// item 1 has most counts
		Assert.assertEquals(new Long(1), (Long)recs.iterator().next());
				

	}
	
	@Test
	public void testRecommendGlobalSimpleDecayedSameTime()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		cr.addCount(1L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
		cr.addCount(4L, 1L, 1L);
		
		Map<Long,Double> rMap =  cr.recommendGlobal(Constants.DEFAULT_DIMENSION, 1,new HashSet<Long>(),1D,null);
		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		Assert.assertEquals(new Long(1), (Long)recs.iterator().next());
				

	}
	
	
	@Test
	public void testRecommendGlobalSimpleDecayed()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(decayClient);
		CountRecommender cr = new CountRecommender(decayClient,userClusters,clusterCount);
		
		TestingUtils.get().setTesting(true);
		TestingUtils.get().setLastActionTime(new Date(5000));
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 5L);
		cr.addCount(1L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
		
		Map<Long,Double> rMap =  cr.recommendGlobal(Constants.DEFAULT_DIMENSION, 1,new HashSet<Long>(),1D,null);
		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// item 2 has most counts
		Assert.assertEquals(new Long(2), (Long)recs.iterator().next());
				

	}
	
	@Test
	public void testRecommendGlobalWithExclusions()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		cr.addCount(1L, 1L, 1L);
		cr.addCount(3L, 1L, 1L);
	
		Set<Long> exclusions = new HashSet<>();
		exclusions.add(1L);
		Map<Long,Double> rMap =  cr.recommendGlobal(Constants.DEFAULT_DIMENSION, 1,exclusions,LONG_DECAY,null);
		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// item 2 has most counts
		Assert.assertEquals(new Long(2), (Long)recs.iterator().next());
				
		
	}
	
	
	@Test
	public void testRecommendSimple()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		
		Map<Long,Double> rMap =  cr.recommend(1L, 0, Constants.DEFAULT_DIMENSION, 1,new HashSet<Long>(),LONG_DECAY);
		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// odd users should prefer odd items
		Assert.assertEquals(new Long(1), (Long)recs.iterator().next());
				
		//even users should prefer even items
		rMap = cr.recommend(2L, 0, Constants.DEFAULT_DIMENSION,1,new HashSet<Long>(),LONG_DECAY);
		recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		Assert.assertEquals(new Long(2), (Long)recs.iterator().next());
	}
	
	@Test
	public void testRecommendMultipleItems()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		cr.addCount(1L, 3L, 1L);
		cr.addCount(1L, 3L, 1L);
		cr.addCount(2L, 4L, 1L);
		cr.addCount(2L, 4L, 1L);

		Set<Long> exclusions = new HashSet<>();
		Map<Long,Double> rMap =  cr.recommend(1L, 0, Constants.DEFAULT_DIMENSION, 1,exclusions,LONG_DECAY);
		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// odd users should prefer odd items
		Assert.assertEquals(new Long(3), (Long)recs.iterator().next());
				

		//even users should prefer even items
		rMap = cr.recommend(2L, 0, Constants.DEFAULT_DIMENSION,1,exclusions,LONG_DECAY);
		recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		Assert.assertEquals(new Long(4), (Long)recs.iterator().next());
	}
	
	@Test
	public void testRecommendSimpleWithExclusions()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		cr.addCount(1L, 3L, 1L);
		cr.addCount(1L, 3L, 1L);
		cr.addCount(2L, 4L, 1L);
		cr.addCount(2L, 4L, 1L);

		Set<Long> exclusions = new HashSet<>();
		exclusions.add(3L);
		Map<Long,Double> rMap =  cr.recommend(1L, 0, Constants.DEFAULT_DIMENSION, 1,exclusions,LONG_DECAY);
		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// odd users should prefer odd items
		Assert.assertEquals(new Long(1), (Long)recs.iterator().next());
				
		exclusions.clear();
		exclusions.add(4L);
		//even users should prefer even items
		rMap = cr.recommend(2L, 0, Constants.DEFAULT_DIMENSION,1,exclusions,LONG_DECAY);
		recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		Assert.assertEquals(new Long(2), (Long)recs.iterator().next());
	}
	
	
	@Test 
	public void testSmallMemorySort()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		cr.addCount(1L, 1L, 1L);
		cr.addCount(2L, 2L, 1L);
		
		List<Long> items = new ArrayList<>();
		items.add(1L);
		items.add(2L);
		
		// odd users should prefer odd items
		List<Long> sorted = cr.sort(1L, items, null);
		Assert.assertEquals(new Long(1), (Long)sorted.iterator().next());
		
		//even users should prefer even items
		sorted = cr.sort(2L, items, null);
		Assert.assertEquals(new Long(2), (Long)sorted.iterator().next());
		
	}
	
	@Test
	public void testMultiThreadMemorySort() throws InterruptedException
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new SimpleUserClusters(2);
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		final CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		final int numThreads = 200;
		final int numIterations = 1000;
		final long maxUsers = 3;
		List<Thread> threads = new ArrayList<>();
		long start = System.currentTimeMillis();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				Random r = new Random();
				for(int j=0;j<numIterations;j++)
				{
					long v = r.nextInt()%maxUsers;
					cr.addCount(v,v, 1L);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		long end = System.currentTimeMillis();
		System.out.println("Time taken msecs:"+(end-start));
		List<Long> items = new ArrayList<>();
		items.add(1L);
		items.add(2L);
		
		// odd users should prefer odd items
		List<Long> sorted = cr.sort(1L, items, null);
		Assert.assertEquals(new Long(1), (Long)sorted.iterator().next());
		
		//even users should prefer even items
		sorted = cr.sort(2L, items, null);
		Assert.assertEquals(new Long(2), (Long)sorted.iterator().next());
		
	}
	
	public static class EmptyUserClusters implements UserClusterStore
	{

		@Override
		public List<UserCluster> getClusters(long userId) {
			return new ArrayList<>();
		}

		@Override
		public int getNumUsersWithClusters() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public List<UserCluster> getClusters() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long getCurrentTimestamp() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean needsExternalCaching() {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
	
	public static class SimpleUserClusters implements UserClusterStore
	{
		int numClusters;
		Map<Long,List<UserCluster>> map = new HashMap<>();
		public SimpleUserClusters(int numClusters)
		{
			this.numClusters = numClusters;
		}
		
		@Override
		public List<UserCluster> getClusters(long userId) {
			List<UserCluster> res = map.get(userId);
			if (res == null)
			{
				res = new ArrayList<>();
				Random r = new Random();
				for(int i=1;i<=numClusters;i++)
				{
					if (userId % 2 == 0 && i % 2 == 0)
						res.add(new UserCluster(userId,i,r.nextFloat(),1,0));
					else if (userId % 2 == 1 && i % 2 == 1)
						res.add(new UserCluster(userId,i,r.nextFloat(),1,0));
				}
				map.put(userId, res);
			}
			return res;
		}

		@Override
		public List<UserCluster> getClusters() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getNumUsersWithClusters() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long getCurrentTimestamp() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean needsExternalCaching() {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
	
}
