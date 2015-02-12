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

package io.seldon.test.tagcount;

import java.util.HashSet;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.similarity.tagcount.ITagCountPeer;
import io.seldon.similarity.tagcount.TagCountRecommender;
import io.seldon.similarity.tagcount.jdo.SqlTagCountPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;

public class TagCountRecommenderTest extends BasePeerTest {

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
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from user_tag");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
		
	
	private void addItemTags(final long itemId,final int attrId,final String tags)
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
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void addUserTagCount(final long userId,final String tag,final int count)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into user_tag (user_id,tag,count) values (?,?,?)");
					query.execute(userId,tag,count);
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
	public void simpleTest()
	{
		try
		{
			addDimension(1, 1, 1);
			addDimensionForItem(1, 1);
			addItem(1);
			addItemTags(1, 9, "tag1,tag2");
			addDimensionForItem(2, 1);
			addItem(2);
			addItemTags(2, 9, "tag2,tag3");
			addDimensionForItem(3, 1);
			addItem(3);
			addItemTags(3, 9, "tag4,tag5");
		
			addUserTagCount(1, "tag1", 4);
			addUserTagCount(1, "tag2", 4);
			
			ITagCountPeer tPeer = new SqlTagCountPeer(props.getClient());
			TagCountRecommender r = new TagCountRecommender(props.getClient(),tPeer);
			Map<Long,Double> res = r.recommend(1, 1, new HashSet<Long>(), 9, 10, 2);
			
			Assert.assertEquals(2, res.size());
			Assert.assertEquals(1.375D, res.get(1L));
			Assert.assertEquals(0.6875D, res.get(2L));
		}
		finally
		{
			clearData();
			MemCachePeer.delete(MemCacheKeys.getItemTags(props.getClient(), 2, 1));
			MemCachePeer.delete(MemCacheKeys.getUserTags(props.getClient(), 1));
		}
		
	}

}
