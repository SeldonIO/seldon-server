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
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.TestingUtils;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.clustering.recommender.jdo.JdoMemoryUserClusterFactory;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.semvec.SemanticVectorsStore;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.CFAlgorithm.CF_SORTER;
import io.seldon.trust.impl.CFAlgorithm.CF_STRATEGY;
import io.seldon.trust.impl.SortResult;
import io.seldon.trust.impl.jdo.RecommendationPeer;



public class SortTest extends BasePeerTest {

		@Autowired
		GenericPropertyHolder props;
		
		@Before
		public void setup()
		{

			SemanticVectorsStore.setBaseDirectory(props.getSemVecBase());
			Properties propsCountUtils = new Properties();
			propsCountUtils.put("io.seldon.clusters.memoryonly", "false");
			JdoCountRecommenderUtils.initialise(propsCountUtils);
			
			Properties testingProps = new Properties();
			testingProps.put("io.seldon.testing", "true");
			TestingUtils.initialise(testingProps);
			MemoryClusterCountFactory.create(new Properties());
			
			JdoMemoryUserClusterFactory.initialise(new Properties()); 
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
		
		private void addClusterCounts(final int id,final long itemId,final double count)
		{
	    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
			try 
			{
				TransactionPeer.runTransaction(new Transaction(pm) { 
				    public void process()
				    { 
				    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into cluster_counts (id,item_id,count,t) values (?,?,?,unix_timestamp(now()))");
						query.execute(id,itemId,count);
				    }});
			} catch (DatabaseException e) 
			{
				logger.error("Failed to clear cluster counts");
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
				    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into user_clusters values (?,?,?)");
						query.execute(userId,clusterId,weight);
						query = pm.newQuery( "javax.jdo.query.SQL","insert ignore into cluster_group values (?,0)");
						query.execute(clusterId);
				    }});
			} catch (DatabaseException e) 
			{
				logger.error("Failed to cluster for user "+userId);
			}
			
		}
		
		@Test
		public void sortTestRankSemanticVectors()
		{
			clearClusterCounts();
			removeClusters();
			
			
			List<Long> recentActions = new ArrayList<>();
			List<Long> items = new ArrayList<>();
			recentActions.add(1L); // i'm a celebrity
			
			items.add(2L); //i'm a celebrity
			addClusterCounts(1,2L,100);
			items.add(5L); //x-factor
			addClusterCounts(1,5L,90);
			
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName("simple");
			List<CF_SORTER> sorters = new ArrayList<>();
			//sorters.add(CF_SORTER.CLUSTER_COUNTS);
			sorters.add(CF_SORTER.SEMANTIC_VECTORS);
			options.setSorters(sorters);
			options.setSorterStrategy(CF_STRATEGY.FIRST_SUCCESSFUL);
			options.setMinNumTxsForSV(0);
			RecommendationPeer recPeer = new RecommendationPeer();
			SortResult result = recPeer.sort(1L, items, options, recentActions);
			
			for(Long item : result.getSortedItems())
				System.out.println("item :"+item);
			
			Assert.assertEquals(5L, (long) result.getSortedItems().get(0));
			Assert.assertEquals(2L, (long) result.getSortedItems().get(1));
			
			
			clearClusterCounts();
			removeClusters();
		}
		
		@Test
		public void sortTesRankClusterOnly()
		{
			clearClusterCounts();
			removeClusters();
			Random r = new Random();
			long userId = r.nextLong();
			updateClusters(userId,1,0.5);
			
			
			List<Long> recentActions = new ArrayList<>();
			List<Long> items = new ArrayList<>();
			recentActions.add(36754L); // i'm a celebrity
			recentActions.add(37272L); //big brother
			recentActions.add(37428L); // x-factor
			recentActions.add(37475L); // nick knowles
			recentActions.add(37577L);// i'm a celebrity
			recentActions.add(37709L); // i'm a celebrity

			items.add(39430L); //i'm a celebrity
			addClusterCounts(1,39430L,100);
			items.add(38141L); //x-factor
			addClusterCounts(1,38141L,90);
			items.add(8840L); //nick knowles
			addClusterCounts(1,8840L,80);
			items.add(37219L); //big brother
			items.add(21425L); //beyonce
			//addClusterCounts(1,21425L,80);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CF_SORTER> sorters = new ArrayList<>();
			sorters.add(CF_SORTER.CLUSTER_COUNTS);
			//sorters.add(CF_SORTER.SEMANTIC_VECTORS);
			options.setSorters(sorters);
			options.setSorterStrategy(CF_STRATEGY.FIRST_SUCCESSFUL);
			RecommendationPeer recPeer = new RecommendationPeer();
			SortResult result = recPeer.sort(userId, items, options, recentActions);
			
			for(Long item : result.getSortedItems())
				System.out.println("item :"+item);
			
			Assert.assertEquals(39430, (long) result.getSortedItems().get(0));
			Assert.assertEquals(38141, (long) result.getSortedItems().get(1));
			
			
			clearClusterCounts();
			removeClusters();
		}
		
		@Test
		public void sortTesRankSum()
		{
			clearClusterCounts();
			removeClusters();
			
			Random r = new Random();
			long userId = r.nextLong();
			updateClusters(userId,1,0.5);
			
			
			List<Long> recentActions = new ArrayList<>();
			List<Long> items = new ArrayList<>();
			recentActions.add(1L); 
			
			items.add(2L); 
			addClusterCounts(1,2L,100);
			items.add(5L); 
			addClusterCounts(1,5L,90);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CF_SORTER> sorters = new ArrayList<>();
			sorters.add(CF_SORTER.CLUSTER_COUNTS);
			sorters.add(CF_SORTER.SEMANTIC_VECTORS);
			options.setSorters(sorters);
			options.setSorterStrategy(CF_STRATEGY.RANK_SUM);
			options.setMinNumTxsForSV(0);
			RecommendationPeer recPeer = new RecommendationPeer();
			SortResult result = recPeer.sort(userId, items, options, recentActions);
			
			for(Long item : result.getSortedItems())
				System.out.println("item :"+item);
			
			Assert.assertEquals(2L, (long) result.getSortedItems().get(0));
			Assert.assertEquals(5L, (long) result.getSortedItems().get(1));
			
			
			clearClusterCounts();
			removeClusters();
		}
	
}
