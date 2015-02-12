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

import java.util.Map;
import java.util.Properties;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.tag.AsyncTagClusterCountFactory;
import io.seldon.clustering.tag.ITagClusterCountStore;
import io.seldon.clustering.tag.jdo.JdoTagClusterCountStore;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.Constants;
import io.seldon.clustering.tag.AsyncTagClusterCountStore;

public class JdoAsyncTagClusterStoreTest  extends BasePeerTest {

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
	
	@After
	public void tearDown()
	{
		AsyncTagClusterCountFactory.get().setActive(props.getClient(), false);
	}
	
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
	public void simpleTest() throws InterruptedException
	{
		try
		{
			final long itemId = 1;
			final int attrId = 9;
			final String tags = "a,b,c";
			addTags(itemId, attrId, tags);

			AsyncTagClusterCountFactory.get().setActive(props.getClient(), true);
			AsyncTagClusterCountFactory.get().get(props.getClient()).addCounts(props.getClient(), Constants.ANONYMOUS_USER, itemId);
			
			Thread.sleep(2000);
			
			ITagClusterCountStore store = new JdoTagClusterCountStore(props.getClient());
			Map<Long,Double> counts = store.getTopCounts("a");
			Assert.assertNotNull(counts);
			Assert.assertEquals(1, counts.size());

		}
		finally
		{
			clearData();
		}
	}

	@Test
	public void activeTest() throws InterruptedException
	{
		try
		{
			final long itemId = 1;
			final int attrId = 9;
			final String tags = "a,b,c";
			addTags(itemId, attrId, tags);

			AsyncTagClusterCountFactory.get().setActive(props.getClient(), false);
			AsyncTagClusterCountStore store = AsyncTagClusterCountFactory.get().get(props.getClient());

			Assert.assertNull(store);

		}
		finally
		{
			AsyncTagClusterCountFactory.get().setActive(props.getClient(), true);
			clearData();
			
		}
	}
}
