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

import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.jdo.AsyncClusterCountFactory;
import io.seldon.clustering.recommender.jdo.JdoMemoryUserClusterFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.clustering.recommender.ClusterCountStore;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.util.CollectionTools;

public class ClusterCountForItemTest  extends BasePeerTest {

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
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into actions values (0,?,?,99,1,now(),0,?,?)");
			    	List<Object> args = new ArrayList<Object>();
			    	args.add(userId);
			    	args.add(itemId);
			    	args.add(""+userId);
			    	args.add(""+itemId);
					query.executeWithArray(args.toArray());
			    }});
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

	
	@Test
	public void lowLevelTestUsingLDAItemClusters()
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		int cluster = 1;
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		MemoryClusterCountFactory.create(clusterProps);
		
		try
		{
			
			final int item1 = 1;
			final int item2 = 2;
			final int item3 = 3;
			
			addItemCluster(item1, 1);
			addItemCluster(item2, 4);
			addItemCluster(item3, 2);
			
			updateClusters(user,cluster,1);
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
			CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
		
			Map<Long,Double> rMap =  cr.recommendUsingItem(item1, Constants.DEFAULT_DIMENSION, 2, new HashSet<Long>(), Integer.MAX_VALUE, CFAlgorithm.CF_CLUSTER_ALGORITHM.LDA_ITEM,0);
			List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
			Assert.assertEquals(2, recs.size());
			Iterator<Long> iter = recs.iterator();
			Assert.assertEquals(new Long(3), iter.next());
			Assert.assertEquals(new Long(2), iter.next());
		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearDimensionData();
			clearItemClusters();
		}
	}
	
	
	@Test
	public void lowLevelTestUsingLDAItemClustersWithDimension()
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		int cluster = 1;
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		MemoryClusterCountFactory.create(clusterProps);
		
		try
		{
			
			final int item1 = 1;
			final int item2 = 2;
			final int item3 = 3;
			
			addItemCluster(item1, 1);
			addItemCluster(item2, 4);
			addItemCluster(item3, 2);
			
			final int dim = 1;
			final int otherDim = 2;

			clearDimensionData();
			addDimension(dim, 1, 1);
			addDimension(otherDim, 1, 2);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, otherDim);

			
			updateClusters(user,cluster,1);
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
			CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
		
			Map<Long,Double> rMap =  cr.recommendUsingItem(item1, dim, 2, new HashSet<Long>(), Integer.MAX_VALUE, CFAlgorithm.CF_CLUSTER_ALGORITHM.LDA_ITEM,0);
			List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
			Assert.assertEquals(2, recs.size());
			Iterator<Long> iter = recs.iterator();
			Assert.assertEquals(new Long(2), iter.next());
			Assert.assertEquals(new Long(1), iter.next());
		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearDimensionData();
			clearItemClusters();
			clearClusterCounts();
		}
	}
	
	@Test
	public void lowLevelTestWithDimensionUsingDimClustersMinAllowedUseRecsAkedFor() // test min allowed recommendations settings
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		int cluster = 1;
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		MemoryClusterCountFactory.create(clusterProps);
		
		try
		{
			final int dim = 1;
			final int otherDim = 2;
			final int item1 = 1;
			final int item2 = 2;
			final int item3 = 3;
			clearDimensionData();
			addDimension(dim, 1, 1);
			addDimension(otherDim, 1, 2);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, otherDim);
			updateClusters(user,cluster,1);
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
			CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
		
			Map<Long,Double> rMap =  cr.recommendUsingItem(item1, dim, 2, new HashSet<Long>(), Integer.MAX_VALUE, CFAlgorithm.CF_CLUSTER_ALGORITHM.DIMENSION,10);
			// recommendations returned even though high min allowed as number of recommendation asked for is lower so this value is used
			List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
			Assert.assertEquals(2, recs.size());
			Iterator<Long> iter = recs.iterator();
			Assert.assertEquals(new Long(2), iter.next());
			Assert.assertEquals(new Long(1), iter.next());

		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearDimensionData();
			clearClusterCounts();
		}
	}
	
	@Test
	public void lowLevelTestWithDimensionUsingDimClustersMinAllowed() // test min allowed recommendations settings
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		int cluster = 1;
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		MemoryClusterCountFactory.create(clusterProps);
		
		try
		{
			final int dim = 1;
			final int otherDim = 2;
			final int item1 = 1;
			final int item2 = 2;
			final int item3 = 3;
			clearDimensionData();
			addDimension(dim, 1, 1);
			addDimension(otherDim, 1, 2);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, otherDim);
			updateClusters(user,cluster,1);
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
			CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
		
			Map<Long,Double> rMap =  cr.recommendUsingItem(item1, dim, 10, new HashSet<Long>(), Integer.MAX_VALUE, CFAlgorithm.CF_CLUSTER_ALGORITHM.DIMENSION,2);
			// recommendations returned even though high min allowed as number of recommendation asked for is lower so this value is used
			List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
			Assert.assertEquals(2, recs.size());
			Iterator<Long> iter = recs.iterator();
			Assert.assertEquals(new Long(2), iter.next());
			Assert.assertEquals(new Long(1), iter.next());

		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearDimensionData();
			clearClusterCounts();
		}
	}
	
	@Test
	public void lowLevelTestWithDimensionUsingDimClustersMinAllowedNoResults() // test min allowed recommendations settings
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		int cluster = 1;
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		MemoryClusterCountFactory.create(clusterProps);
		
		try
		{
			final int dim = 1;
			final int otherDim = 2;
			final int item1 = 1;
			final int item2 = 2;
			final int item3 = 3;
			clearDimensionData();
			addDimension(dim, 1, 1);
			addDimension(otherDim, 1, 2);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, otherDim);
			updateClusters(user,cluster,1);
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
			CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
		
			Map<Long,Double> rMap =  cr.recommendUsingItem(item1, dim, 10, new HashSet<Long>(), Integer.MAX_VALUE, CFAlgorithm.CF_CLUSTER_ALGORITHM.DIMENSION,4);
			Assert.assertEquals(0, rMap.size());

		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearDimensionData();
			clearClusterCounts();
		}
	}
	
	@Test
	public void lowLevelTestWithDimensionUsingDimClusters()
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		int cluster = 1;
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		MemoryClusterCountFactory.create(clusterProps);
		
		try
		{
			final int dim = 1;
			final int otherDim = 2;
			final int item1 = 1;
			final int item2 = 2;
			final int item3 = 3;
			clearDimensionData();
			addDimension(dim, 1, 1);
			addDimension(otherDim, 1, 2);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, otherDim);
			updateClusters(user,cluster,1);
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
			CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
		
			Map<Long,Double> rMap =  cr.recommendUsingItem(item1, dim, 2, new HashSet<Long>(), Integer.MAX_VALUE, CFAlgorithm.CF_CLUSTER_ALGORITHM.DIMENSION,0);
			List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
			Assert.assertEquals(2, recs.size());
			Iterator<Long> iter = recs.iterator();
			Assert.assertEquals(new Long(2), iter.next());
			Assert.assertEquals(new Long(1), iter.next());
		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearDimensionData();
			clearClusterCounts();
		}
	}

	
	@Test
	public void lowLevelTestWithDimensionUsingDimClustersAndSignificantAlg()
	{
		Random rand = new Random();
		Long user = rand.nextLong();
		Long otherUser = rand.nextLong();
		int cluster = 1;
		int otherCluster = 2;
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
		MemoryClusterCountFactory.create(new Properties());
		
		try
		{
			final int dim = 1;
			final int otherDim = 2;
			final int item1 = 1;
			final int item2 = 2;
			final int item3 = 3;
			clearDimensionData();
			addDimension(dim, 1, 1);
			addDimension(otherDim, 1, 2);
			addDimensionForItem(item1, dim);
			addDimensionForItem(item2, dim);
			addDimensionForItem(item3, otherDim);
			updateClusters(user,cluster,1);
			updateClusters(otherUser,otherCluster,1);
			
			clusterProps = new Properties();
			clusterProps.put("io.seldon.memoryuserclusters.clients", props.getClient());
			JdoMemoryUserClusterFactory factory = JdoMemoryUserClusterFactory.initialise(clusterProps);
		
			JdoCountRecommenderUtils u = new JdoCountRecommenderUtils(props.getClient());
			CountRecommender cr = u.getCountRecommender(props.getClient());
			cr.setRecommenderType(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_SIGNIFICANT);
		
			cr.addCount(user, item1);
			cr.addCount(user, item1);
			cr.addCount(user, item2);
			cr.addCount(user, item2);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			cr.addCount(user, item3);
			
			cr.addCount(otherUser, item1);
			cr.addCount(otherUser, item2);
			cr.addCount(otherUser, item2);
			cr.addCount(otherUser, item2);
		
			final double decay = 43200;
			updateClustersTotals(TestingUtils.getTime(), decay);
			
			Map<Long,Double> rMap =  cr.recommendUsingItem(item1, dim, 2, new HashSet<Long>(), Integer.MAX_VALUE, CFAlgorithm.CF_CLUSTER_ALGORITHM.DIMENSION,0);
			List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
			Assert.assertEquals(2, recs.size());
			Iterator<Long> iter = recs.iterator();
			Assert.assertEquals(new Long(1), iter.next());
			Assert.assertEquals(new Long(2), iter.next());
		}
		finally
		{
			MemoryClusterCountFactory.create(new Properties());
			removeClusters();
			clearDimensionData();
			clearClusterCounts();
		}
	}

	

}
