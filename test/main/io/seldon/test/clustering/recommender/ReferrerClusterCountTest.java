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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.Util;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.db.jdo.Transaction;
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
import io.seldon.clustering.recommender.ClusterFromReferrerPeer;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.UserClusterStore;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.clustering.recommender.CountRecommenderTest.EmptyUserClusters;
import io.seldon.util.CollectionTools;

public class ReferrerClusterCountTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	private static final int clusterId = -10;
	private static final String googleReferrerHost = "http://www.google.com";
	
	private void createClusterCountFactory()
	{
		Properties clusterProps = new Properties();
		clusterProps.put("io.seldon.memoryclusters.clients", props.getClient());
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".type", "SIMPLE");
		clusterProps.put("io.seldon.memoryclusters."+props.getClient()+".threshold", "0");
	
	
		MemoryClusterCountFactory.create(clusterProps);
	}
	
	@Before
	public void setup()
	{
		this.createClusterCountFactory();
		Properties testingProps = new Properties();
		testingProps.put("io.seldon.testing", "true");
		TestingUtils.initialise(testingProps);
		addReferrer(googleReferrerHost, clusterId);
		Properties cprops = new Properties();
		cprops.setProperty(ClusterFromReferrerPeer.PROP, props.getClient());
		ClusterFromReferrerPeer.initialise(cprops);
	}
	
	@After 
	public void tearDown()
	{
		MemoryClusterCountFactory.create(new Properties());
		Properties testingProps = new Properties();
		testingProps.put("io.seldon.testing", "false");
		TestingUtils.initialise(testingProps);
		clearClusterReferrer();
	}
	
	
	private void addReferrer(final String referrer,final int cluster)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into cluster_referrer (referrer,cluster) values (?,?)");
					query.execute(referrer,cluster);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void clearClusterReferrer()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from  cluster_referrer");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
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
	
	@Test
	public void testAnonymousReferrerClusters()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new EmptyUserClusters();
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<>();
        CFAlgorithm alg = new CFAlgorithm();
        map.put(props.getClient(), alg);
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
		
        cr.setReferrer(googleReferrerHost);
		cr.addCount(1L, 1L, 1L);
		
		Map<Long,Double> rMap =  cr.recommend(Constants.ANONYMOUS_USER, 0, Constants.DEFAULT_DIMENSION, 1, new HashSet<Long>(), 99999999);

		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// item 1 has most counts
		Assert.assertEquals(new Long(1), (Long)recs.iterator().next());
	}
	
	
	@Test
	public void testReferrerClusters()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new EmptyUserClusters();
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<>();
        CFAlgorithm alg = new CFAlgorithm();
        map.put(props.getClient(), alg);
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
		
        cr.setReferrer(googleReferrerHost);
		cr.addCount(1L, 1L, 1L);
		
		Map<Long,Double> rMap =  cr.recommend(1L, 0, Constants.DEFAULT_DIMENSION, 1, new HashSet<Long>(), 99999999);

		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// item 1 has most counts
		Assert.assertEquals(new Long(1), (Long)recs.iterator().next());
	}
	
	@Test
	public void testReferrerClusters2()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new EmptyUserClusters();
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<>();
        CFAlgorithm alg = new CFAlgorithm();
        map.put(props.getClient(), alg);
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
		
        cr.setReferrer(googleReferrerHost);
		cr.addCount(1L, 1L, 1L);
		cr.setReferrer(googleReferrerHost+"/someStuff/stuff2");
		Map<Long,Double> rMap =  cr.recommend(1L, 0, Constants.DEFAULT_DIMENSION, 1, new HashSet<Long>(), 99999999);

		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		// item 1 has most counts
		Assert.assertEquals(new Long(1), (Long)recs.iterator().next());
	}
	
	
	@Test
	public void testEmptyReferrerClusters()
	{
		this.createClusterCountFactory();
		UserClusterStore userClusters = new EmptyUserClusters();
		ClusterCountStore clusterCount = MemoryClusterCountFactory.get().getStore(props.getClient());
		CountRecommender cr = new CountRecommender(props.getClient(),userClusters,clusterCount);
		
		AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<>();
        CFAlgorithm alg = new CFAlgorithm();
        map.put(props.getClient(), alg);
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
		
        cr.setReferrer(googleReferrerHost);
		cr.addCount(1L, 1L, 1L);
		cr.setReferrer(null);
		Map<Long,Double> rMap =  cr.recommend(1L, 0, Constants.DEFAULT_DIMENSION, 1, new HashSet<Long>(), 99999999);

		List<Long> recs = CollectionTools.sortMapAndLimitToList(rMap, rMap.size());
		Assert.assertEquals(0, recs.size());
	}

}
