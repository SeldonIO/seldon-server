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

import java.util.Random;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.tag.jdo.JdoItemTagCache;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.clustering.tag.jdo.ItemTags;
import io.seldon.db.jdo.DatabaseException;

public class JdoItemTagCacheTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	private void clearData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from  tag_cluster_counts");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from item_map_varchar");
					query.execute();
				}
			});
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

	
	@Test
	public void simpleTest()
	{
		Random r = new Random();
		final int attrId = 9;
		final long itemId = r.nextLong();
		final String memcacheKey = MemCacheKeys.getItemTags(props.getClient(), itemId, attrId);
		try
		{
			
			final String tags = "a,b,c";
			addTags(itemId, attrId, tags);
			JdoItemTagCache store = new JdoItemTagCache(props.getClient());
			Set<String> tagsFound = store.getTags(itemId, attrId);
			Assert.assertNotNull(tagsFound);
			Assert.assertEquals(3, tagsFound.size());
			Assert.assertTrue(tagsFound.contains("a"));
			Assert.assertTrue(tagsFound.contains("b"));
			Assert.assertTrue(tagsFound.contains("c"));
			
			// check the result has been put into memcache
			ItemTags it = (ItemTags) MemCachePeer.get(memcacheKey);
			Assert.assertNotNull(it);

		}
		finally
		{
			MemCachePeer.delete(memcacheKey);
			clearData();
		}
	}

	
}
