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

package io.seldon.test.peer;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.Constants;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.general.Item;
import io.seldon.general.User;

public class ItemPeerTest extends BasePeerTest  {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;
	
	
	private void clearItemData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from dimension");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from item_map_enum");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from items");
					query.execute();
				}
			});
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
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into items (item_id,last_op) values (?,now())");
					query.execute(itemId);
					query.closeAll();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
	
	@Test @Ignore
    public void testItemsFromActions()
	{
		Collection<User> users = userPeer.getActiveUsers(1);
		for(User u : users)
		{
			Collection<Item> items =itemPeer.getItemsFromUserActions(u.getUserId(), "fblike", 10);
			for(Item i : items)
				System.out.println("Item:"+i.getName());
		}
	}
	
	
	@Test 
	public void testGetMinItemId() throws InterruptedException
	{
		try
		{
			ConsumerBean c = new ConsumerBean();
			c.setShort_name(props.getClient());
			addItem(1);
			Thread.sleep(1000);
			addItem(2);
			Collection<Item> items = itemPeer.getRecentItems(1, Constants.DEFAULT_DIMENSION, c);
			if (items.size() > 0)
			{
				Item i = items.iterator().next();
				long itemId = itemPeer.getMinItemId(i.getLastOp(), Constants.DEFAULT_DIMENSION, c);
				Assert.assertEquals(i.getItemId(), itemId);
			}
		}
		finally
		{
			clearItemData();
		}
	}
}
