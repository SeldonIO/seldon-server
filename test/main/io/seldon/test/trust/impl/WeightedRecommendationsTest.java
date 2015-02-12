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
import java.util.Properties;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.jdo.AsyncClusterCountFactory;
import io.seldon.clustering.recommender.jdo.JdoMemoryUserClusterFactory;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RecommendationResult;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.clustering.recommender.ClusterCountStore;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.db.jdo.DatabaseException;

public class WeightedRecommendationsTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;
	
	@Before
	public void setup()
	{
		Properties props1 = new Properties();
		props1.put("io.seldon.actioncache.clients", props.getClient());
		ActionHistoryCache.initalise(props1);
		
		Properties propsR = new Properties();
		propsR.put("io.seldon.clusters.memoryonly", "false");
		JdoCountRecommenderUtils.initialise(propsR);
		
		Properties testingProps = new Properties();
		testingProps.put("io.seldon.testing", "true");
		TestingUtils.initialise(testingProps);
		AsyncClusterCountFactory.create(new Properties());
	}
	
	@After 
	public void tearDown()
	{
		MemoryClusterCountFactory.create(new Properties());
		JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(new Properties());
		Properties testingProps = new Properties();
		testingProps.put("io.seldon.testing", "false");
		TestingUtils.initialise(testingProps);

	}
	
	private void addAction(final long userId,final long itemId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "insert into actions values (0,?,?,99,1,now(),0,?,?)");
					List<Object> args = new ArrayList<Object>();
					args.add(userId);
					args.add(itemId);
					args.add("" + userId);
					args.add("" + itemId);
					query.executeWithArray(args.toArray());
				}
			});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
	
	/**
	 * Cleanup method to remove user from minhashuser table
	 * @param userId
	 */
	private void removeActions(final long userId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from actions where user_id=?");
					query.execute(userId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear actions");
		}
	}
	
	private void removeClusters()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","delete from user_clusters;");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL","delete from user_clusters_transient;");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clusters");
		}
		
	}
	
	private void updateClusters(final long userId,final int clusterId,final double weight)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","delete from user_clusters where user_id=?");
					query.execute(userId);
			    	query = pm.newQuery( "javax.jdo.query.SQL","insert into user_clusters values (?,?,?)");
					query.execute(userId,clusterId,weight);
					query = pm.newQuery( "javax.jdo.query.SQL","insert ignore into cluster_group values (?,0)");
					query.execute(clusterId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to cluster for user "+userId);
		}
		
	}
	
	private void clearClusterCounts()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from  cluster_counts");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	@Test
	public void combineClusterWithGlobalTest()
	{
		for(int i=0;i<2;i++)
		{
			Random rand = new Random();
			Long user = rand.nextLong();
			Long userOther = rand.nextLong();
			ActionHistoryCache c = new ActionHistoryCache(props.getClient());
			Properties clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
			clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
			clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
			MemoryClusterCountFactory.create(clusterProps);
			try
			{

				int cluster = 1;
				int cluster2 = 2;
				updateClusters(user,cluster,1);
				updateClusters(userOther,cluster2,1);
				clusterProps = new Properties();
				clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
				JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
			
				ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
				CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
			
				cr.addCount(user, 1L);
				cr.addCount(user, 1L);
				cr.addCount(user, 1L);
				cr.addCount(user, 3L);
				cr.addCount(user, 3L);
				
				cr.addCount(userOther, 2L);
				cr.addCount(userOther, 2L);
				cr.addCount(userOther, 2L);
				cr.addCount(userOther, 2L);
				
				cr.addCount(userOther, 3L);
				cr.addCount(userOther, 3L);
				cr.addCount(userOther, 3L);
				cr.addCount(userOther, 3L);
				cr.addCount(userOther, 3L);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_GLOBAL);
			options.setMaxRecommendersToUse(2);
			options.setRecommenders(recommenders);
			if (i==0)
				options.setRecommenderStrategy(CFAlgorithm.CF_STRATEGY.WEIGHTED);
			else
				options.setRecommenderStrategy(CFAlgorithm.CF_STRATEGY.RANK_SUM);
			System.out.println("Testing with strategy "+options.getRecommenderStrategy().name());
			RecommendationPeer recPeer = new RecommendationPeer();
			
			RecommendationResult rres = recPeer.getRecommendations(user, ""+user,0, Constants.DEFAULT_DIMENSION, 2, options,null,null,null);
			List<Recommendation> recs = rres.getRecs();
			
			Assert.assertEquals(2, recs.size());
			
			Recommendation r = recs.get(0);
			Assert.assertEquals(3L, r.getContent());
			
			r = recs.get(1);
			Assert.assertEquals(1L, r.getContent());
			
			}
			finally
			{
				//
				// cleanup 
				//
				MemoryClusterCountFactory.create(new Properties());
				c.removeRecentActions(user);
				removeClusters();
				removeActions(user);
				clearClusterCounts();
			}
		}
	}
	
	@Test
	public void combineClusterWithGlobalTestWeighted()
	{
		for(int i=0;i<2;i++)
		{
			Random rand = new Random();
			Long user = rand.nextLong();
			Long userOther = rand.nextLong();
			ActionHistoryCache c = new ActionHistoryCache(props.getClient());
			Properties clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
			clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
			clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
			MemoryClusterCountFactory.create(clusterProps);
			try
			{

				int cluster = 1;
				int cluster2 = 2;
				updateClusters(user,cluster,1);
				updateClusters(userOther,cluster2,1);
				clusterProps = new Properties();
				clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
				JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
			
				ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
				CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
			
				cr.addCount(user, 1L);
				cr.addCount(user, 1L);
				cr.addCount(user, 1L);
				cr.addCount(user, 3L);
				cr.addCount(user, 3L);
				
				cr.addCount(userOther, 3L);
				cr.addCount(userOther, 3L);
				cr.addCount(userOther, 3L);
				cr.addCount(userOther, 3L);
				cr.addCount(userOther, 3L);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS);
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_GLOBAL);
			options.setMaxRecommendersToUse(2);
			options.setRecommenders(recommenders);
			if (i==0)
				options.setRecommenderStrategy(CFAlgorithm.CF_STRATEGY.WEIGHTED);
			else
				options.setRecommenderStrategy(CFAlgorithm.CF_STRATEGY.RANK_SUM);
			System.out.println("Testing with strategy "+options.getRecommenderStrategy().name());
			Map<CFAlgorithm.CF_RECOMMENDER,Double> weights = new HashMap<CFAlgorithm.CF_RECOMMENDER,Double>();
			weights.put(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS, 1.0D);
			weights.put(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_GLOBAL, 0.01D);
			options.setRecommendationWeightMap(weights);
			RecommendationPeer recPeer = new RecommendationPeer();
			
			RecommendationResult rres = recPeer.getRecommendations(user, ""+user,0, Constants.DEFAULT_DIMENSION, 2, options,null,null,null);
			List<Recommendation> recs = rres.getRecs();
			
			Assert.assertEquals(2, recs.size());
			
			Recommendation r = recs.get(0);
			Assert.assertEquals(1L, r.getContent()); // 1L first as the weight for global is set very low
			
			r = recs.get(1);
			Assert.assertEquals(3L, r.getContent());
			
			}
			finally
			{
				//
				// cleanup 
				//
				MemoryClusterCountFactory.create(new Properties());
				c.removeRecentActions(user);
				removeClusters();
				removeActions(user);
				clearClusterCounts();
			}
		}
	}

}
