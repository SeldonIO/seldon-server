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

package io.seldon.test.similarity.item;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.TestingUtils;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.similarity.item.IItemSimilarityPeer;
import io.seldon.similarity.item.ItemSimilarityRecommender;
import io.seldon.similarity.item.JdoItemSimilarityPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.CFAlgorithm.CF_RECOMMENDER;
import io.seldon.trust.impl.CFAlgorithm.CF_STRATEGY;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RecommendationResult;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import io.seldon.util.CollectionTools;

public class ItemSimilarityRecommenderTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;
	
	private void clearData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from items");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from dimension");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_map_enum");
					query.execute();
			    	query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_map_varchar");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from recommendations");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_similarity");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_similarity_historical");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
		
	
	
	private void addRecommendation(final long userId,final long itemId,final double score)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into recommendations (user_id,item_id,score) values (?,?,?)");
					query.execute(userId,itemId,score);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void addItemSimilarity(final long itemId,final long itemId2,final double score)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{

			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into item_similarity (item_id,item_id2,score) values (?,?,?)");
					query.execute(itemId,itemId2,score);
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
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into dimension (dim_id,item_type,attr_id,value_id) values (?,0,?,?)");
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
	
	private void addItem(final long itemId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into items (item_id,client_item_id,first_op) values (?,?,now())");
					query.execute(itemId,""+itemId);
					query.closeAll();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to add item",e);
		}
	}
	
	@Test
	public void checkSimilarityOrderingInDb()
	{
		try
		{
			Random r = new Random();
			Long item = r.nextLong();
			
			addDimension(1, 1, 1);

			addDimensionForItem(1, 1);
			addItem(1);
			addItemSimilarity(item, 1, 1.0);

			addDimensionForItem(2, 1);
			addItem(2);
			addItemSimilarity(2, item, 0.5);
			
			IItemSimilarityPeer peer = new JdoItemSimilarityPeer(props.getClient());
			ItemSimilarityRecommender recommender = new ItemSimilarityRecommender(props.getClient(),peer);
			
			Map<Long,Double> res = recommender.recommendSimilarItems(item, 1, 3, new HashSet<Long>());
			Assert.assertEquals(2, res.size());
			List<Long> recs = CollectionTools.sortMapAndLimitToList(res, 2, true);
			Assert.assertEquals((long)1, (long)recs.get(0));
			Assert.assertEquals((long)2, (long)recs.get(1));
			
		}
		finally
		{
			clearData();
		}
	}
	
	
	@Test
	public void orderedStrategyTest() throws InterruptedException
	{
		try
		{
			TestingUtils.get().setTesting(true);
			Random r = new Random();
			Long user = r.nextLong();
			
			addDimension(1, 1, 1);
			
			addDimensionForItem(1, 1);
			addItem(1);
			addRecommendation(user, 1, 1.0);
			
			addItem(2);
			addDimensionForItem(2, 1);
			
			Thread.sleep(2000);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CF_RECOMMENDER> recommenders = new ArrayList<CF_RECOMMENDER>();
			recommenders.add(CF_RECOMMENDER.ITEM_SIMILARITY_RECOMENDER);
			recommenders.add(CF_RECOMMENDER.RECENT_ITEMS);
			options.setRecommenders(recommenders);
			options.setRecommenderStrategy(CF_STRATEGY.ORDERED);
			System.out.println("Testing with strategy "+options.getRecommenderStrategy().name());
			RecommendationPeer recPeer = new RecommendationPeer();
			
			RecommendationResult rres = recPeer.getRecommendations(user, ""+user,0, 1, 2, options,null,null,null);
			List<Recommendation> recs = rres.getRecs();
			
			Assert.assertEquals(2, recs.size());
			for(Recommendation rec : recs)
				System.out.println("Recommended item "+rec.getContent());
			
		}
		finally
		{
			clearData();
			TestingUtils.get().setTesting(false);
		}
	}
	
	
	@Test
	public void orderedStrategyTestNotEnoughRecs() throws InterruptedException
	{
		// check even though only 2 recs can be returned when we ask for 3, 2 are still retruned rather than an error or none
		try
		{
			TestingUtils.get().setTesting(true);
			Random r = new Random();
			Long user = r.nextLong();
			
			addDimension(1, 1, 1);
			
			addDimensionForItem(1, 1);
			addItem(1);
			addRecommendation(user, 1, 1.0);
			
			addItem(2);
			addDimensionForItem(2, 1);
			
			Thread.sleep(2000);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CF_RECOMMENDER> recommenders = new ArrayList<CF_RECOMMENDER>();
			recommenders.add(CF_RECOMMENDER.ITEM_SIMILARITY_RECOMENDER);
			recommenders.add(CF_RECOMMENDER.RECENT_ITEMS);
			options.setRecommenders(recommenders);
			options.setRecommenderStrategy(CF_STRATEGY.ORDERED);
			System.out.println("Testing with strategy "+options.getRecommenderStrategy().name());
			RecommendationPeer recPeer = new RecommendationPeer();
			
			RecommendationResult rres = recPeer.getRecommendations(user, ""+user,0, 1, 3, options,null,null,null);
			List<Recommendation> recs = rres.getRecs();
			
			for(Recommendation rec : recs)
				System.out.println("Recommended item "+rec.getContent());
			Assert.assertEquals(2, recs.size());
			
			
		}
		finally
		{
			clearData();
			TestingUtils.get().setTesting(false);
		}
	}
	
	@Test 
	public void simpleLowLevelRecommenderTest()
	{
		try
		{
			Random r = new Random();
			Long user = r.nextLong();
			
			addDimension(1, 1, 1);
			
			addDimensionForItem(1, 1);
			addItem(1);
			addRecommendation(user, 1, 1.0);
			addDimensionForItem(2, 1);
			addItem(2);
			addRecommendation(user, 2, 0.5);
			addDimensionForItem(3, 1);
			addItem(3);
			
			IItemSimilarityPeer peer = new JdoItemSimilarityPeer(props.getClient());
			ItemSimilarityRecommender recommender = new ItemSimilarityRecommender(props.getClient(),peer);
			
			Map<Long,Double> res = recommender.recommend(user, 1, 3, new HashSet<Long>());
			Assert.assertEquals(2, res.size());
			List<Long> recs = CollectionTools.sortMapAndLimitToList(res, 2, true);
			Assert.assertEquals((long)1, (long)recs.get(0));
			Assert.assertEquals((long)2, (long)recs.get(1));
			
		}
		finally
		{
			clearData();
		}
	}
	
	
	@Test 
	public void simpleLowLevelRecommenderTestWithExclusions()
	{
		try
		{
			Random r = new Random();
			Long user = r.nextLong();
			
			addDimension(1, 1, 1);
			
			addDimensionForItem(1, 1);
			addItem(1);
			addRecommendation(user, 1, 1.0);
			addDimensionForItem(2, 1);
			addItem(2);
			addRecommendation(user, 2, 0.5);
			addDimensionForItem(3, 1);
			addItem(3);
			
			Set<Long> excl = new HashSet<Long>();
			excl.add(1L);
			IItemSimilarityPeer peer = new JdoItemSimilarityPeer(props.getClient());
			ItemSimilarityRecommender recommender = new ItemSimilarityRecommender(props.getClient(),peer);
			
			Map<Long,Double> res = recommender.recommend(user, 1, 3, excl);
			Assert.assertEquals(1, res.size());
			List<Long> recs = CollectionTools.sortMapAndLimitToList(res, 2, true);
			Assert.assertEquals((long)2, (long)recs.get(0));
			
		}
		finally
		{
			clearData();
		}
	}
	
	@Test 
	public void simpleLowLevelRecommenderTestWithExclusionsAndDimensions()
	{
		try
		{
			Random r = new Random();
			Long user = r.nextLong();
			
			addDimension(1, 1, 1);
			addDimension(2, 1, 2);
			
			addDimensionForItem(1, 1);
			addItem(1);
			addRecommendation(user, 1, 1.0);
			addDimensionForItem(2, 2);
			addItem(2);
			addRecommendation(user, 2, 0.5);
			addDimensionForItem(3, 1);
			addItem(3);
			addRecommendation(user, 3, 0.25);
			
			Set<Long> excl = new HashSet<Long>();
			excl.add(1L);
			IItemSimilarityPeer peer = new JdoItemSimilarityPeer(props.getClient());
			ItemSimilarityRecommender recommender = new ItemSimilarityRecommender(props.getClient(),peer);
			
			Map<Long,Double> res = recommender.recommend(user, 1, 3, excl);
			Assert.assertEquals(1, res.size());
			List<Long> recs = CollectionTools.sortMapAndLimitToList(res, 2, true);
			Assert.assertEquals((long)3, (long)recs.get(0));
			
		}
		finally
		{
			clearData();
		}
	}
	
	
	
	@Test 
	public void simpleLowLevelItemSimilarityTest()
	{
		try
		{
			Random r = new Random();
			Long item = r.nextLong();
			
			addDimension(1, 1, 1);

			addDimensionForItem(1, 1);
			addItem(1);
			addItemSimilarity(item, 1, 1.0);

			addDimensionForItem(2, 1);
			addItem(2);
			addItemSimilarity(item, 2, 0.5);
			
			IItemSimilarityPeer peer = new JdoItemSimilarityPeer(props.getClient());
			ItemSimilarityRecommender recommender = new ItemSimilarityRecommender(props.getClient(),peer);
			
			Map<Long,Double> res = recommender.recommendSimilarItems(item, 1, 3, new HashSet<Long>());
			Assert.assertEquals(2, res.size());
			List<Long> recs = CollectionTools.sortMapAndLimitToList(res, 2, true);
			Assert.assertEquals((long)1, (long)recs.get(0));
			Assert.assertEquals((long)2, (long)recs.get(1));
			
		}
		finally
		{
			clearData();
		}
	}
	
	
	
	
	@Test 
	public void simpleLowLevelItemSimilarityTestWithExclusions()
	{
		try
		{
			Random r = new Random();
			Long item = r.nextLong();
			
			addDimension(1, 1, 1);

			addDimensionForItem(1, 1);
			addItem(1);
			addItemSimilarity(item, 1, 1.0);

			addDimensionForItem(2, 1);
			addItem(2);
			addItemSimilarity(item, 2, 0.5);
			
			Set<Long> excl = new HashSet<Long>();
			excl.add(1L);
			IItemSimilarityPeer peer = new JdoItemSimilarityPeer(props.getClient());
			ItemSimilarityRecommender recommender = new ItemSimilarityRecommender(props.getClient(),peer);
			
			Map<Long,Double> res = recommender.recommendSimilarItems(item, 1, 3, excl);
			Assert.assertEquals(1, res.size());
			List<Long> recs = CollectionTools.sortMapAndLimitToList(res, 2, true);
			Assert.assertEquals((long)2, (long)recs.get(0));
			
		}
		finally
		{
			clearData();
		}
	}
	
	@Test 
	public void simpleLowLevelItemSimilarityTestWithExclusionsAnddimensions()
	{
		try
		{
			Random r = new Random();
			Long item = r.nextLong();
			
			addDimension(1, 1, 1);
			addDimension(2, 1, 2);

			addDimensionForItem(1, 1);
			addItem(1);
			addItemSimilarity(item, 1, 1.0);

			addDimensionForItem(2, 2);
			addItem(2);
			addItemSimilarity(item, 2, 0.5);

			addDimensionForItem(3, 1);
			addItem(3);
			addItemSimilarity(item, 3, 0.25);
			
			Set<Long> excl = new HashSet<Long>();
			excl.add(1L);
			IItemSimilarityPeer peer = new JdoItemSimilarityPeer(props.getClient());
			ItemSimilarityRecommender recommender = new ItemSimilarityRecommender(props.getClient(),peer);
			
			Map<Long,Double> res = recommender.recommendSimilarItems(item, 1, 3, excl);
			Assert.assertEquals(1, res.size());
			List<Long> recs = CollectionTools.sortMapAndLimitToList(res, 2, true);
			Assert.assertEquals((long)3, (long)recs.get(0));
			
		}
		finally
		{
			clearData();
		}
	}
	
	@Test 
	public void simpleLowLevelItemSimilarityTestWithExclusionsAnddimensionsDbOrder()
	{
		try
		{
			Random r = new Random();
			Long item = r.nextLong();
			
			addDimension(1, 1, 1);
			addDimension(2, 1, 2);

			addDimensionForItem(1, 1);
			addItem(1);
			addItemSimilarity(item, 1, 1.0);

			addDimensionForItem(2, 2);
			addItem(2);
			addItemSimilarity(2, item, 0.5);

			addDimensionForItem(3, 1);
			addItem(3);
			addItemSimilarity(3, item, 0.25);
			
			Set<Long> excl = new HashSet<Long>();
			excl.add(1L);
			IItemSimilarityPeer peer = new JdoItemSimilarityPeer(props.getClient());
			ItemSimilarityRecommender recommender = new ItemSimilarityRecommender(props.getClient(),peer);
			
			Map<Long,Double> res = recommender.recommendSimilarItems(item, 1, 3, excl);
			Assert.assertEquals(1, res.size());
			List<Long> recs = CollectionTools.sortMapAndLimitToList(res, 2, true);
			Assert.assertEquals((long)3, (long)recs.get(0));
			
		}
		finally
		{
			clearData();
		}
	}
}
