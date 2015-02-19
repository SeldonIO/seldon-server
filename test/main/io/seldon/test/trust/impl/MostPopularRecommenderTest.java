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
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

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
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class MostPopularRecommenderTest extends BasePeerTest {

			@Autowired
			GenericPropertyHolder props;
			
			@Autowired PersistenceManager pm;

			private void clearData()
			{
		    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
				try 
				{
					TransactionPeer.runTransaction(new Transaction(pm) {
						public void process() {
							Query query = pm.newQuery("javax.jdo.query.SQL", "delete from items_popular");
							query.execute();
							query = pm.newQuery("javax.jdo.query.SQL", "delete from dimension");
							query.execute();
							query = pm.newQuery("javax.jdo.query.SQL", "delete from item_map_enum");
							query.execute();
							query = pm.newQuery("javax.jdo.query.SQL", "delete from item_attr");
							query.execute();

						}
					});
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
			public void simpleTest()
			{
				Random rand = new Random();
				final int dimension = 1;
				try
				{
					final long item1 = 1L;
					final long item2 = 2L;
					final long item3 = 3L;
					long userId = rand.nextLong();
					addDimension(dimension, 1, 1);
					addDimensionForItem(item1, dimension);
					addDimensionForItem(item2, dimension);
					addDimensionForItem(item3, dimension);
					addItemPopularity(item1, 10);
					addItemPopularity(item2, 5);
					addItemPopularity(item3, 1);
					
					CFAlgorithm options = new CFAlgorithm();
					options.setName(props.getClient());
					List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<>();
					recommenders.add(CFAlgorithm.CF_RECOMMENDER.MOST_POPULAR);
					options.setRecommenders(recommenders);
					options.setSorterStrategy(CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL);
					RecommendationPeer recPeer = new RecommendationPeer();
					RecommendationResult res = recPeer.getRecommendations(userId,""+userId, 1, dimension, 10, options, null, null,null);
					List<Recommendation> r = res.getRecs();
					
					Assert.assertEquals(3, r.size());
					Assert.assertEquals(item1,r.get(0).getContent());
					Assert.assertEquals(item2,r.get(1).getContent());
					Assert.assertEquals(item3,r.get(2).getContent());
					
					
				}
				finally
				{
					clearData();
					String mkey = MemCacheKeys.getMostPopularItems(props.getClient(), dimension);
					MemCachePeer.delete(mkey);
				}
			}
			
			
			@Test
			public void simpleTestWithExclusions()
			{
				Random rand = new Random();
				final int dimension = 1;
				try
				{
					final long item1 = 1L;
					final long item2 = 2L;
					final long item3 = 3L;
					long userId = rand.nextLong();
					addDimension(dimension, 1, 1);
					addDimensionForItem(item1, dimension);
					addDimensionForItem(item2, dimension);
					addDimensionForItem(item3, dimension);
					addItemPopularity(item1, 10);
					addItemPopularity(item2, 5);
					addItemPopularity(item3, 1);
					
					CFAlgorithm options = new CFAlgorithm();
					options.setName(props.getClient());
					List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<>();
					recommenders.add(CFAlgorithm.CF_RECOMMENDER.MOST_POPULAR);
					options.setRecommenders(recommenders);
					options.setSorterStrategy(CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL);
					RecommendationPeer recPeer = new RecommendationPeer();
					RecommendationResult res = recPeer.getRecommendations(userId,""+userId, 1, dimension, 10, options, null, item1,null);
					List<Recommendation> r = res.getRecs();
					
					Assert.assertEquals(2, r.size());
					Assert.assertEquals(item2,r.get(0).getContent());
					Assert.assertEquals(item3,r.get(1).getContent());
					
				}
				finally
				{
					clearData();
					String mkey = MemCacheKeys.getMostPopularItems(props.getClient(), dimension);
					MemCachePeer.delete(mkey);
				}
			}

			
			@Test
			public void simpleTestWithDimensions()
			{
				Random rand = new Random();
				final int dimension1 = 1;
				final int dimension2 = 2;
				try
				{
					final long item1 = 1L;
					final long item2 = 2L;
					final long item3 = 3L;
					CFAlgorithm options = new CFAlgorithm();
					Long userId = rand.nextLong();
					addDimension(dimension1, 1, 1);
					addDimension(dimension2, 1, 2);
					addAttrEnum(1, options.getCategoryDim());
					addDimensionForItem(item1, dimension1);
					addDimensionForItem(item2, dimension2);
					addDimensionForItem(item3, dimension2);
					addItemPopularity(item1, 10);
					addItemPopularity(item2, 5);
					addItemPopularity(item3, 1);
					
					
					options.setName(props.getClient());
					List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<>();
					recommenders.add(CFAlgorithm.CF_RECOMMENDER.MOST_POPULAR_ITEM_CATEGORY);
					options.setRecommenders(recommenders);
					options.setSorterStrategy(CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL);
					RecommendationPeer recPeer = new RecommendationPeer();
					RecommendationResult res = recPeer.getRecommendations(userId,""+userId, 1, dimension1, 10, options, null, item2,null);
					List<Recommendation> r = res.getRecs();
					
					Assert.assertEquals(1, r.size());
					Assert.assertEquals(item3,r.get(0).getContent());
					
				}
				finally
				{
					clearData();
					String mkey = MemCacheKeys.getMostPopularItems(props.getClient(), dimension1);
					MemCachePeer.delete(mkey);
					mkey = MemCacheKeys.getMostPopularItems(props.getClient(), dimension2);
					MemCachePeer.delete(mkey);
				}
			}

			

}
