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

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.Transaction;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.memcache.MemCachePeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RecommendationResult;

public class RecentItemsRecommenderTest  extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;
	

	private void addItemPopularity(final long itemId,final int count)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into items_popular (item_id,opsum) values (?,?)");
					query.execute(itemId,count);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	private void clearItemData()
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
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from items");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from items_popular");
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
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into items (item_id,client_item_id) values (?,?)");
					query.execute(itemId,""+itemId);
					query.closeAll();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
	
	private void clearState()
	{
		clearItemData();
		CFAlgorithm options = new CFAlgorithm();
		options.setName(props.getClient());
		String key = MemCacheKeys.getRecentItems(options.getName(), 1, options.getRecentArticlesForSV());
		MemCachePeer.delete(key);
	}
	
	@Test
	public void simpleTest()
	{
	    clearState();
		try
		{
			addDimension(1, 1, 1);
			addDimensionForItem(1, 1);
			addItem(1);
			addDimensionForItem(2, 1);
			addItem(2);
			addDimensionForItem(3, 1);
			addItem(3);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<>();
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.RECENT_ITEMS);
			options.setRecommenders(recommenders);
			options.setSorterStrategy(CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL);
			RecommendationPeer recPeer = new RecommendationPeer();
			RecommendationResult res = recPeer.getRecommendations(1L,""+1L, 1, 1, 10, options, null, null,null);
			
			Assert.assertEquals(3, res.getRecs().size());
			List<Recommendation> recs = res.getRecs();
			Assert.assertEquals(3, recs.get(0).getContent());
			Assert.assertEquals(2, recs.get(1).getContent());
			Assert.assertEquals(1, recs.get(2).getContent());
		}
		finally
		{
			clearState();
		}
	}
	
	@Test
	public void simpleTestWithExclusion()
	{
		try
		{
			addDimension(1, 1, 1);
			addDimensionForItem(1, 1);
			addItem(1);
			addDimensionForItem(2, 1);
			addItem(2);
			addDimensionForItem(3, 1);
			addItem(3);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<>();
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.RECENT_ITEMS);
			options.setRecommenders(recommenders);
			options.setSorterStrategy(CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL);
			RecommendationPeer recPeer = new RecommendationPeer();
			RecommendationResult res = recPeer.getRecommendations(1L,""+1L, 1, 1, 10, options, null, 1L, null);
			
			Assert.assertEquals(2, res.getRecs().size());
			List<Recommendation> recs = res.getRecs();
			Assert.assertEquals(3, recs.get(0).getContent());
			Assert.assertEquals(2, recs.get(1).getContent());
			
		}
		finally
		{
			clearState();
		}
	}

	
	@Test
	public void simpleTestWithMostPopularReordering()
	{
	    clearState();
		try
		{
			addDimension(1, 1, 1);
			addDimensionForItem(1, 1);
			addItem(1);
			addItemPopularity(1, 10);
			addDimensionForItem(2, 1);
			addItem(2);
			addItemPopularity(2, 5);
			addDimensionForItem(3, 1);
			addItem(3);
			addItemPopularity(3, 1);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<>();
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.RECENT_ITEMS);
			options.setRecommenders(recommenders);
			options.setSorterStrategy(CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL);
			options.setPostprocessing(CFAlgorithm.CF_POSTPROCESSING.REORDER_BY_POPULARITY);
			RecommendationPeer recPeer = new RecommendationPeer();
			RecommendationResult res = recPeer.getRecommendations(1L,""+1L, 1, 1, 10, options, null, null, null);
			
			Assert.assertEquals(3, res.getRecs().size());
			List<Recommendation> recs = res.getRecs();
			Assert.assertEquals(1, recs.get(0).getContent());
			Assert.assertEquals(2, recs.get(1).getContent());
			Assert.assertEquals(3, recs.get(2).getContent());
		}
		finally
		{
			clearState();
		}
	}
}
