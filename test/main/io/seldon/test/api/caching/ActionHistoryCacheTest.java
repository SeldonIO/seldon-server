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

package io.seldon.test.api.caching;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ActionHistoryCacheTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;
	
	@Before
	public void setup()
	{
		Properties props1 = new Properties();
		props1.put("io.seldon.actioncache.clients", props.getClient());
		ActionHistoryCache.initalise(props1);
	}
	
	private void addAction(final long userId,final long itemId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "insert into actions values (0,?,?,99,1,now(),0,?,?)");
					List<Object> args = new ArrayList<>();
					args.add(userId);
					args.add(itemId);
					args.add("" + userId);
					args.add("" + itemId);
					query.executeWithArray(args.toArray());
				}
			});
		} catch (DatabaseException e)
		{
			logger.error("Failed to clear cluster counts");
		}
	}
	
	/**
	 * Cleanup method to remove user from minhashuser table
	 * @param userId
	 */
	private void removeActions(final long userId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from actions where user_id=?");
					query.execute(userId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear actions");
		}
	}
	
	@Test 
	public void testCacheNotActiveExpire1() throws InterruptedException
	{
		Properties props1 = new Properties();
		props1.put("io.seldon.actioncache.clients", props.getClient());
		int timeout = 2;
		props1.put("io.seldon.actioncache.timeout", ""+timeout);
		ActionHistoryCache.initalise(props1);
		
		ActionHistoryCache.setClient(props.getClient(), false);
		Random r = new Random();
		long userId = r.nextLong();
		long itemId1 = r.nextLong();
		long itemId2 = r.nextLong();
		addAction(userId,itemId1);
		ActionHistoryCache c = new ActionHistoryCache(props.getClient());
		List<Long> actions = c.getRecentActions(userId, 200);
		Assert.assertEquals(0, actions.size()); // db search not done
		addAction(userId,itemId2);
		
		c.addAction(userId, itemId2);
		actions = c.getRecentActions(userId, 200);
		Assert.assertEquals(1, actions.size()); //cache has not expired
		//sleep for cache expire time
		Thread.sleep(timeout*1000+1000);
		actions = c.getRecentActions(userId, 200);
		Assert.assertEquals(0, actions.size()); // expired from cache and not got again from db
		
		//cleanup
		ActionHistoryCache.setClient(props.getClient(), true);
		removeActions(userId);
	}
	
	
	
	@Test
	public void testActionsAlreadyInDBNotInCache()
	{
		Random r = new Random();
		long userId = r.nextLong();
		long itemId1 = r.nextLong();
		long itemId2 = r.nextLong();
		addAction(userId,itemId1);
		ActionHistoryCache c = new ActionHistoryCache(props.getClient());
		c.addAction(userId, itemId2);
		List<Long> actions = c.getRecentActions(userId, 2);
		Assert.assertEquals(1, actions.size()); // only cached one
		for(Long action : actions)
			System.out.println("Action "+action);
		Assert.assertEquals(itemId2, (long) actions.get(0)); //newer added last
		
		//cleanup
		removeActions(userId);
	}
	
	@Test
	public void testAddAndGet()
	{
		Random r = new Random();
		long userId = r.nextLong();
		long item1 = 1;
		long item2 = 2;
		ActionHistoryCache c = new ActionHistoryCache(props.getClient());
		c.addAction(userId, item1);
		c.addAction(userId, item2);
		List<Long> actions = c.getRecentActions(userId, 2);
		Assert.assertEquals(2, actions.size());
		for(Long action : actions)
			System.out.println("Action "+action);
	}
	
	@Test
	public void testGetBeforeAdd()
	{
		long userId = 4435433;
		long item1 = 1;
		long item2 = 2;
		ActionHistoryCache c = new ActionHistoryCache(props.getClient());
		List<Long> actions = c.getRecentActions(userId, 2);
		Assert.assertEquals(0, actions.size());
		for(Long action : actions)
			System.out.println("Action "+action);

	}
	
	@Test
	public void testReturnSizeLimited()
	{
		Random r = new Random();
		long userId = r.nextLong();
		long item1 = 1;
		long item2 = 2;
		long item3 = 3;
		ActionHistoryCache c = new ActionHistoryCache(props.getClient());
		c.addAction(userId, item1);
		c.addAction(userId, item2);
		c.addAction(userId, item3);
		List<Long> actions = c.getRecentActions(userId, 2);
		for(Long action : actions)
			System.out.println("Action "+action);
		Assert.assertEquals(2, actions.size());
	}
}
