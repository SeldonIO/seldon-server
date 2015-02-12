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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.Constants;
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
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.util.CollectionTools;

public class ClusterCountSignificantTest   extends BasePeerTest {

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
			logger.error("Failed to clear cluster counts");
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
	
	private void updateClustersTotals(final long time,final double decay)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into cluster_counts_total select 1,sum(exp(-(greatest(?-t,0)/?))*count),unix_timestamp() from cluster_counts");
					query.execute(time,decay);
			    	query = pm.newQuery( "javax.jdo.query.SQL","insert into cluster_counts_item_total select item_id,sum(exp(-(greatest(?-t,0)/?))*count),unix_timestamp() from cluster_counts group by item_id");
					query.execute(time,decay);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to update cluster total");
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
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from  cluster_counts_total");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from  cluster_counts_item_total");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	@Test
	public void anonymousUserTest()
	{
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
		JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
	
		JdoCountRecommenderUtils u = new JdoCountRecommenderUtils(props.getClient());
		CountRecommender cr = u.getCountRecommender(props.getClient());
		cr.setRecommenderType(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_SIGNIFICANT);
	
		
		final double decay = 43200;
		updateClustersTotals(TestingUtils.getTime(), decay);
		Map<Long,Double> rMap = cr.recommend(Constants.ANONYMOUS_USER, null, 1, 3, new HashSet<Long>(), false, 1.0, 1.0, decay, 1);
		
		Assert.assertNotNull(rMap);
		Assert.assertEquals(0, rMap.size());
		
	}
	
	@Test
	public void lowLevelTest()
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		Long otherUser = rand.nextLong();
		int cluster1 = 1;
		int cluster2 = 2;
		
		Properties clusterProps = new Properties();

		try
		{

			final int dim = 1;
			final int numRecs = 2;

			final int item1 = rand.nextInt();
			final int item2 = rand.nextInt();
			final int item3 = rand.nextInt();
			clearDimensionData();
			addDimension(dim, 1, 1);
			updateClusters(user,cluster1,1);
			updateClusters(otherUser,cluster2,1);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, dim);
			
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			JdoCountRecommenderUtils u = new JdoCountRecommenderUtils(props.getClient());
			CountRecommender cr = u.getCountRecommender(props.getClient());
			cr.setRecommenderType(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_SIGNIFICANT);
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);

			cr.addCount(otherUser, item1);
			cr.addCount(otherUser, item3);
			cr.addCount(otherUser, item3);
			cr.addCount(otherUser, item3);

			final double decay = 43200;
			updateClustersTotals(TestingUtils.getTime(), decay);
			Map<Long,Double> rMap = cr.recommend(user, null, dim, 3, new HashSet<Long>(), false, 1.0, 1.0, decay, 1);
			
			List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
			Assert.assertEquals(3, recs.size());
			Iterator<Long> iter = recs.iterator();
			//
			// results show that high count negative correlation items like item3 are ranked lower due to 
			// current scoring of percen-changet * count - if percent-change is negative
			Assert.assertEquals(new Long(item2), iter.next());
			Assert.assertEquals(new Long(item3), iter.next());
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
	
	

}
