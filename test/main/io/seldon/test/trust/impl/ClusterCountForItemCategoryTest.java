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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.TestingUtils;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.jdo.AsyncClusterCountFactory;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.clustering.recommender.jdo.JdoMemoryUserClusterFactory;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RecommendationResult;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import io.seldon.util.CollectionTools;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ClusterCountForItemCategoryTest  extends BasePeerTest {

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
	
	private void clearDimensionData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from dimension");
					query.execute();
			    	query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_map_enum");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	
	private void addDimension(final int dimension,final int attrId,final int valueId)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into dimension (dim_id,attr_id,value_id) values (?,?,?)");
					query.execute(dimension,attrId,valueId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void addItemCluster(final long itemId,final int clusterId)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into item_clusters (item_id,cluster_id,weight,created) values (?,?,1.0,unix_timestamp())");
					query.execute(itemId,clusterId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	
	private void clearItemClusters()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_clusters");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void addDimensionForItem(final long itemId,final int dimension)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into item_map_enum (item_id,attr_id,value_id) select ?,attr_id,value_id from dimension where dim_id=?");
					query.execute(itemId,dimension);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts",e);
		}
		
	}
	
	private void addAttrEnum(final int attrId,final String name)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into item_attr (attr_id,name) values (?,?)");
					query.execute(attrId,name);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts",e);
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
	public void lowLevelTest()
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		int cluster = 1;
		Properties clusterProps = new Properties();
		//clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		//clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		//clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		//MemoryClusterCountFactory.create(clusterProps);
		
		try
		{

			final int dim = 1;
			
			final int catDim1 = 2;
			final int catDim2 = 3;

			final int numRecs = 2;
			String memcacheKey = MemCacheKeys.getTopClusterCountsForDimension(props.getClient(), dim, catDim1, numRecs);
			MemCachePeer.delete(memcacheKey);
			
			final int item1 = rand.nextInt();
			final int item2 = rand.nextInt();
			final int item3 = rand.nextInt();
			clearDimensionData();
			addDimension(dim, 1, 1);
			addDimension(catDim1, 2, 1);
			addDimension(catDim2, 2, 2);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, dim);
			addDimensionForItem(item1, catDim1);
			addDimensionForItem(item2, catDim1);
			addDimensionForItem(item3, catDim2);
			updateClusters(user,cluster,1);
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			JdoCountRecommenderUtils u = new JdoCountRecommenderUtils(props.getClient());
			CountRecommender cr = u.getCountRecommender(props.getClient());
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
		
			Map<Long,Double> rMap =  cr.recommendGlobal(dim, numRecs, new HashSet<Long>(), Integer.MAX_VALUE, catDim1);
			List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
			Assert.assertEquals(2, recs.size());
			Iterator<Long> iter = recs.iterator();
			Assert.assertEquals(new Long(item2), iter.next());
			Assert.assertEquals(new Long(item1), iter.next());
		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearClusterCounts();
			clearDimensionData();
		}
	}
	
	
	@Test
	public void highLevelTest()
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		int cluster = 1;
		Properties clusterProps = new Properties();
		//clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		//clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		//clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		//MemoryClusterCountFactory.create(clusterProps);
		
		try
		{

			final int dim = 1;
			
			final int catDim1 = 2;
			final int catDim2 = 3;

			final int numRecs = 2;
			String memcacheKey = MemCacheKeys.getTopClusterCountsForDimension(props.getClient(),dim,catDim1,numRecs);
			MemCachePeer.delete(memcacheKey);
			
			final long item1 = rand.nextInt();
			final long item2 = rand.nextInt();
			final long item3 = rand.nextInt();
			final long item4 = rand.nextInt();
			clearDimensionData();
			addDimension(dim, 1, 1);
			addAttrEnum(2,"category");
			addDimension(catDim1, 2, 1);
			addDimension(catDim2, 2, 2);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, dim);
			addDimensionForItem(item1, catDim1);
			addDimensionForItem(item2, catDim1);
			addDimensionForItem(item3, catDim2);
			addDimensionForItem(item4, catDim1);
			
			updateClusters(user,cluster,1);
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			JdoCountRecommenderUtils u = new JdoCountRecommenderUtils(props.getClient());
			CountRecommender cr = u.getCountRecommender(props.getClient());
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
		

			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_ITEM_CATEGORY);
			options.setMaxRecommendersToUse(1);
			options.setRecommenders(recommenders);
			options.setRecommenderStrategy(CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL);
			
			RecommendationPeer recPeer = new RecommendationPeer();
			
			RecommendationResult rres = recPeer.getRecommendations(0, "ab",0, 1, 2, options,null,item4,null);
			List<Recommendation> recs = rres.getRecs();
			
			Assert.assertEquals(2, recs.size());
			Iterator<Recommendation> iter = recs.iterator();
			Assert.assertEquals(item2, iter.next().getContent());
			Assert.assertEquals(item1, iter.next().getContent());
		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearClusterCounts();
			clearDimensionData();
		}
	}

}
