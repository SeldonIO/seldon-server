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

package io.seldon.test.clustering.tag.jdo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.Constants;
import io.seldon.clustering.tag.AsyncTagClusterCountFactory;
import io.seldon.clustering.tag.IItemTagCache;
import io.seldon.clustering.tag.ITagClusterCountStore;
import io.seldon.clustering.tag.TagClusterRecommender;
import io.seldon.clustering.tag.jdo.JdoItemTagCache;
import io.seldon.clustering.tag.jdo.JdoTagClusterCountStore;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RecommendationResult;
import io.seldon.trust.impl.CFAlgorithm.CF_RECOMMENDER;
import io.seldon.trust.impl.CFAlgorithm.CF_STRATEGY;
import io.seldon.trust.impl.jdo.RecommendationPeer;

public class TagCountRecommenderTest  extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Before 
	public void setup()
	{
		Properties cprops = new Properties();
		cprops.setProperty("io.seldon.tag.async.counter.active", "true");
		cprops.setProperty("io.seldon.tag.async.counter.start", props.getClient());
		cprops.setProperty("io.seldon.tag.async.counter."+props.getClient()+".qtimeout", "1");
		AsyncTagClusterCountFactory.create(cprops);
	}
	
	private void clearData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from  tag_cluster_counts");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_map_varchar");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from items");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void addTags(final long itemId,final int attrId,final String tags)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into item_map_varchar (item_id,attr_id,value) values (?,?,?)");
					query.execute(itemId,attrId,tags);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to add tags",e);
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

	
	private void addItem(final long itemId)
	{
		final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
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

	
	@Test
	public void simpleTest() throws InterruptedException
	{
		try
		{
			final int dim = 1;
			final Long itemId1 = 1L;
			final Long itemId2 = 2L;
			final int attrId = 9;
			final String tags = "a,b,c";
			final String tags2 = "a";
			addDimension(dim, 1, 1);
			addDimensionForItem(itemId1, dim);
			addDimensionForItem(itemId2, dim);
			addTags(itemId1, attrId, tags);
			addTags(itemId2, attrId, tags2);

			AsyncTagClusterCountFactory.get().get(props.getClient()).addCounts(props.getClient(), Constants.ANONYMOUS_USER, itemId1);
			AsyncTagClusterCountFactory.get().get(props.getClient()).addCounts(props.getClient(), Constants.ANONYMOUS_USER, itemId1);
			AsyncTagClusterCountFactory.get().get(props.getClient()).addCounts(props.getClient(), Constants.ANONYMOUS_USER, itemId2);
			
			Thread.sleep(2000);
			
			ITagClusterCountStore store = new JdoTagClusterCountStore(props.getClient());
			IItemTagCache tagCache = new JdoItemTagCache(props.getClient());
			
			TagClusterRecommender r = new TagClusterRecommender(props.getClient(), store, tagCache, attrId);
			Map<Long,Double> res = r.recommend(-1, itemId2, new HashSet<Long>(), dim, 2,new HashSet<Long>(), 999999);
			
			Assert.assertNotNull(res);
			Assert.assertEquals(2, res.size());
			Assert.assertEquals(1.0D, res.get(itemId1));
			Assert.assertTrue(res.containsKey(itemId2));
			Assert.assertTrue(res.get(itemId1) > res.get(itemId2));
			

		}
		finally
		{
			clearData();
			clearDimensionData();
		}
	}
	
	@Test
	public void highLevelTestWithAnonUser() throws InterruptedException
	{
		try
		{
			final int dim = 1;
			final Long itemId1 = 1L;
			final Long itemId2 = 2L;
			final int attrId = 9;
			final String tags = "a,b,c";
			final String tags2 = "a";
			addDimension(dim, 1, 1);
			addDimensionForItem(itemId1, dim);
			addDimensionForItem(itemId2, dim);
			addTags(itemId1, attrId, tags);
			addTags(itemId2, attrId, tags2);

			AsyncTagClusterCountFactory.get().get(props.getClient()).addCounts(props.getClient(), Constants.ANONYMOUS_USER, itemId1);
			AsyncTagClusterCountFactory.get().get(props.getClient()).addCounts(props.getClient(), Constants.ANONYMOUS_USER, itemId1);
			AsyncTagClusterCountFactory.get().get(props.getClient()).addCounts(props.getClient(), Constants.ANONYMOUS_USER, itemId2);
			
			Thread.sleep(2000);
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CF_RECOMMENDER> recommenders = new ArrayList<CF_RECOMMENDER>();
			recommenders.add(CF_RECOMMENDER.TAG_CLUSTER_COUNTS);
			options.setMaxRecommendersToUse(1);
			options.setRecommenders(recommenders);
			options.setRecommenderStrategy(CF_STRATEGY.FIRST_SUCCESSFUL);
			options.setTagUserHistory(5);
			
			RecommendationPeer recPeer = new RecommendationPeer();
			
			RecommendationResult rres = recPeer.getRecommendations(Constants.ANONYMOUS_USER, "ab",0, dim, 2, options,null,itemId1,null);
			List<Recommendation> recs = rres.getRecs();
			
			Assert.assertNotNull(recs);
			Assert.assertEquals(1, recs.size());
			

		}
		finally
		{
			clearData();
			clearDimensionData();
		}
	}

}
