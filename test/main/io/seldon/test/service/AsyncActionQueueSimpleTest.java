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

package io.seldon.test.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.service.async.JdoAsyncActionQueue;
import io.seldon.db.jdbc.JDBCConnectionFactory;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.Action;
import io.seldon.general.Item;
import io.seldon.general.User;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;

public class AsyncActionQueueSimpleTest extends BasePeerTest {

	@Autowired PersistenceManager pm;
	
	@Autowired
	GenericPropertyHolder props;
	
	@Before
	public void setup()
	{
		removeActions();
	}
	
	@After
	public void tearDown()
	{
		removeActions();
	}
	
	private Long getNumUsersWithZeroId()
	{
		final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		Query query = pm.newQuery( "javax.jdo.query.SQL","select count(*) from actions where user_id=0");
		query.setUnique(true);
		query.setResultClass(Long.class);
		Long num = (Long) query.execute();
		query.closeAll();
		return num;
	}
	
	private void removeActions()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","delete from actions");
					query.execute();
			    	query = pm.newQuery( "javax.jdo.query.SQL","delete from users");
					query.execute();
			    	query = pm.newQuery( "javax.jdo.query.SQL","delete from items");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clusters");
		}
		
	}
	
	 private void addActionBatch(PreparedStatement actionPreparedStatement,Action action) throws SQLException
	    {
	    	actionPreparedStatement.setLong(1, action.getUserId());
			actionPreparedStatement.setLong(2, action.getItemId());
			if (action.getType() != null)
				actionPreparedStatement.setInt(3, action.getType());
			else
				actionPreparedStatement.setNull(3, java.sql.Types.INTEGER);
			
			if (action.getTimes() != null)
				actionPreparedStatement.setInt(4, action.getTimes());
			else
				actionPreparedStatement.setNull(4, java.sql.Types.INTEGER);
			
			if (action.getDate() != null)
				actionPreparedStatement.setTimestamp(5, new java.sql.Timestamp(action.getDate().getTime()));
			else
				actionPreparedStatement.setNull(5, java.sql.Types.TIMESTAMP);
			
			if (action.getValue() != null)
				actionPreparedStatement.setDouble(6, action.getValue());
			else
				actionPreparedStatement.setNull(6, java.sql.Types.DOUBLE);
			
			actionPreparedStatement.setString(7, action.getClientUserId());
			actionPreparedStatement.setString(8, action.getClientItemId());
	    }
	 
	@Test 
	public void testExceptionInBatch() throws SQLException
	{
		try
		{
		Random r = new Random();
		Connection connection = JDBCConnectionFactory.get().getConnection(props.getClient());
		connection.setAutoCommit( false );
		connection.setReadOnly(true);
		Action a = new Action(0L,0L,1L,89,1,new Date(),0D,""+r.nextInt(),"2");
		
		PreparedStatement actionPreparedStatement = connection.prepareStatement("insert into actions (action_id,user_id,item_id,type,times,date,value,client_user_id,client_item_id) values (0,?,?,?,?,?,?,?,?)");
		addActionBatch(actionPreparedStatement,a);
		actionPreparedStatement.addBatch();
		
		try
		{
			actionPreparedStatement.executeBatch();
		}
		catch (Exception e)
		{
			logger.info("Got exception",e);
		}
		connection.setReadOnly(false);
		actionPreparedStatement.executeBatch();
		actionPreparedStatement.close();
		connection.commit();
		
		Collection<Action> actions = actionPeer.getRecentActions(10);
		int size = actions.size();
		
		Assert.assertEquals(1, size);
		}
		finally
		{
		removeActions();
		}
	}
	
	
	@Test
	public void testIdUpdatedWhenOnlyOneUpdater() throws InterruptedException
	{
		
		try
		{
		Random rand = new Random();
		final long userId = rand.nextInt(100000);
		final int actionType = 89;
		final int numActions = 20000;
		
		final JdoAsyncActionQueue actionQueue1 = new JdoAsyncActionQueue(props.getClient(), 1, 2500000,Integer.MAX_VALUE,3,true,false,true);
		final JdoAsyncActionQueue actionQueue2 = new JdoAsyncActionQueue(props.getClient(), 1, 2500000,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner1 = new Thread(actionQueue1);
		queueRunner1.start();
		Thread queueRunner2 = new Thread(actionQueue2);
		queueRunner2.start();
		
		Runnable r = new Runnable() { public void run()
		{
			Random r = new Random();
			for(int i=0;i<numActions;i++)
			{
				Action a = new Action(0L,0L,0L,89,1,new Date(),0D,""+r.nextInt(),""+r.nextInt());
				if (r.nextFloat() <= 0.5)
					actionQueue1.put(a);
				else
					actionQueue2.put(a);
			
				
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			}
		}
		};
		Thread t = new Thread(r);
		t.start();
		

		
		t.join();
		
		Thread.sleep(5000); // give queues time to flush sql
		
		actionQueue1.setKeepRunning(false);
		actionQueue2.setKeepRunning(false);
		queueRunner1.join();
		queueRunner2.join();
		
		Collection<Action> actions = actionPeer.getRecentActions(numActions);
		int size = actions.size();
		Assert.assertEquals(numActions, size);
		
		Collection<User> users = userPeer.getRecentUsers(numActions);
		size = users.size();
		Assert.assertEquals(numActions,size);

		Collection<Item> items = itemPeer.getRecentItems(numActions, 0, new ConsumerBean(props.getClient()));
		size = items.size();
		Assert.assertEquals(numActions,size);
		
		long numUsersWithZeroId = getNumUsersWithZeroId();
		Assert.assertEquals(0, numUsersWithZeroId);
		
		}
		finally
		{
		removeActions();
		}
	}
	
	
	@Test
	public void testForDeadlocks() throws InterruptedException
	{
		
		try
		{
		Random rand = new Random();
		final long userId = rand.nextInt(100000);
		final int actionType = 89;
		final int numActions = 20000;
		
		final JdoAsyncActionQueue actionQueue1 = new JdoAsyncActionQueue(props.getClient(), 1, 2500000,Integer.MAX_VALUE,3,true,true,true);
		final JdoAsyncActionQueue actionQueue2 = new JdoAsyncActionQueue(props.getClient(), 1, 2500000,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner1 = new Thread(actionQueue1);
		queueRunner1.start();
		Thread queueRunner2 = new Thread(actionQueue2);
		queueRunner2.start();
		
		Runnable r = new Runnable() { public void run()
		{
			Random r = new Random();
			for(int i=0;i<numActions;i++)
			{
				Action a = new Action(0L,0L,0L,89,1,new Date(),0D,""+r.nextInt(),""+r.nextInt());
				if (r.nextFloat() <= 0.5)
					actionQueue1.put(a);
				else
					actionQueue2.put(a);
			
				
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			}
		}
		};
		Thread t = new Thread(r);
		t.start();
		

		
		t.join();
		
		Thread.sleep(5000); // give queues time to flush sql
		
		actionQueue1.setKeepRunning(false);
		actionQueue2.setKeepRunning(false);
		queueRunner1.join();
		queueRunner2.join();
		
		Collection<Action> actions = actionPeer.getRecentActions(numActions);
		int size = actions.size();
		Assert.assertEquals(numActions, size);
		
		Collection<User> users = userPeer.getRecentUsers(numActions);
		size = users.size();
		Assert.assertEquals(numActions,size);

		Collection<Item> items = itemPeer.getRecentItems(numActions, 0, new ConsumerBean(props.getClient()));
		size = items.size();
		Assert.assertEquals(numActions,size);
		
		}
		finally
		{
		removeActions();
		}
	}
	
	@Test 
	public void ensureContinuousWrites() throws InterruptedException
	{
		try
		{
		Random rand = new Random();
		final long userId = rand.nextInt(100000);
		final int actionType = 89;
		final int numActions = 200;
		
		final JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 2500000,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		Runnable r = new Runnable() { public void run()
		{
			for(int i=0;i<numActions;i++)
			{
				Action a = new Action(0L,userId,1L,actionType,1,new Date(),0D,""+userId,"2");
				actionQueue.put(a);
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		};
		Thread t = new Thread(r);
		t.start();
		
		Thread.sleep(4000);
		
		Collection<Action> actions = actionPeer.getRecentUserActions(userId, actionType, numActions);
		int size = actions.size();
		Assert.assertTrue(size>0);
		Assert.assertTrue(size < numActions);
		
		t.join();
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		actions = actionPeer.getRecentUserActions(userId, actionType, numActions);
		
		Assert.assertEquals(numActions, actions.size());
		}
		finally
		{
		removeActions();
		}
	}
	
	
	
	@Test 
	public void testIdsTooLong() throws InterruptedException
	{
		String clientUserId = "";
		for(int i=0;i<JdoAsyncActionQueue.MAX_CLIENT_USER_ID_LEN+10;i++)
			clientUserId = clientUserId + "a";
		
		String clientItemId = "";
		for(int i=0;i<JdoAsyncActionQueue.MAX_CLIENT_ITEM_ID_LEN+10;i++)
			clientItemId = clientItemId + "a";
		
		JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 25,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
			
		Action a = new Action(0L,1L,1L,0,0,new Date(),0D,clientUserId,"1");
		actionQueue.put(a);
		
		a = new Action(0L,1L,1L,0,0,new Date(),0D,"1",clientItemId);
		actionQueue.put(a);
			
		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
			
		Assert.assertEquals(2, actionQueue.getBadActions());
	}
	
	
	
	@Test 
	public void testSingleThreaded() throws InterruptedException
	{
		try
		{
		int numActions = 100;
		long userId = 1L;
		int actionType = 98;
		
		JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 25,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		for(int i=0;i<numActions;i++)
		{
			Action a = new Action(0L,userId,2L,actionType,1,new Date(),0D,"clientUser"+userId,"clientItem2");
			actionQueue.put(a);
		}
		
		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		Collection<Action> actions = actionPeer.getRecentUserActions(userId, actionType, numActions);
		
		Assert.assertEquals(numActions, actions.size());
		}
		finally
		{
		removeActions();
		}
	}
	
	@Test 
	public void testSingleThreadedNewUsers() throws InterruptedException
	{
		try
		{
		int numActions = 100;
		long userId = 0L;
		String clientUserId = "user1";
		int actionType = 99;
		
		JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 25,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		for(int i=0;i<numActions;i++)
		{
			Action a = new Action(0L,userId,1L,actionType,1,new Date(),0D,clientUserId,"1");
			actionQueue.put(a);
		}
		
		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		Collection<Action> actions = actionPeer.getRecentUserActions(clientUserId, actionType, numActions);
		
		Assert.assertEquals(numActions, actions.size());
		}
		finally
		{
		removeActions();
		}
	}
	
	@Test 
	public void testSingleThreadedNewUsersNewItems() throws InterruptedException
	{
		try
		{
		int numActions = 100;
		long userId = 0L;
		String clientUserId = "user1";
		int actionType = 97;
		
		JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 25,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		for(int i=0;i<numActions;i++)
		{
			Action a = new Action(0L,userId,0L,actionType,1,new Date(),0D,clientUserId,"1");
			actionQueue.put(a);
		}
		
		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		Collection<Action> actions = actionPeer.getRecentUserActions(clientUserId, actionType, numActions);
		
		Assert.assertEquals(numActions, actions.size());
		}
		finally
		{
		removeActions();
		}
	}
	
	
	@Test 
	public void testMultiThreadedNewUser() throws InterruptedException
	{
		try
		{
		final int numActions = 100;
		final long userId = 0L;
		final int actionType = 95;
		int numThreads = 20;
		List<Thread> threads = new ArrayList<>();
		
		final JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 1000,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		long start = System.currentTimeMillis();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int i=0;i<numActions;i++)
				{
					Action a = new Action(0L,userId,1L,actionType,1,new Date(),0D,""+userId,"1");
					actionQueue.put(a);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();

		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		long end = System.currentTimeMillis();
		System.out.println("Time taken msecs:"+(end-start));
		
		Collection<Action> actions = actionPeer.getRecentUserActions(""+userId, actionType, numActions*numThreads);
		
		Assert.assertEquals(numActions*numThreads, actions.size());
		}
		finally
		{
		removeActions();
		}
	}
	
	
	@Test
	public void testMultiThreaded() throws InterruptedException
	{
		try
		{
		final int numActions = 1000;
		final long userId = 1L;
		final int actionType = 94;
		int numThreads = 20;
		List<Thread> threads = new ArrayList<>();
		
		final JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 1000,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		long start = System.currentTimeMillis();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				for(int i=0;i<numActions;i++)
				{
					Action a = new Action(0L,userId,1L,actionType,1,new Date(),0D,""+userId,"1");
					actionQueue.put(a);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();
		
		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		long end = System.currentTimeMillis();
		System.out.println("Time taken msecs:"+(end-start));
		
		Collection<Action> actions = actionPeer.getRecentUserActions(userId, actionType, numActions*numThreads);
		
		Assert.assertEquals(numActions*numThreads, actions.size());
		}
		finally
		{
		removeActions();
		}
	}
	
	@Test 
	public void testMultiThreadedMultiUser() throws InterruptedException
	{
		try
		{
		final int numActions = 1000;
		final int maxUsers = 100;
		final int actionType = 93;
		int numThreads = 20;
		List<Thread> threads = new ArrayList<>();
		
		final JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 1000,Integer.MAX_VALUE,3,true,true,true);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		long start = System.currentTimeMillis();
		for(int i=0;i<numThreads;i++)
		{
			Runnable r = new Runnable() { public void run()
			{
				Random r = new Random();
				for(int i=0;i<numActions;i++)
				{
					long userId = r.nextInt(maxUsers) +1;
					Action a = new Action(0L,0L,1L,actionType,1,new Date(),0D,""+userId,"1");
					actionQueue.put(a);
				}
			}
			};
			Thread t = new Thread(r);
			threads.add(t);
			t.start();
		}
		
		for(Thread t : threads)
			t.join();

		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		long end = System.currentTimeMillis();
		System.out.println("Time taken msecs:"+(end-start));
		}
		finally
		{
		removeActions();
		}
	}
	
	@Test 
	public void testNoInsertActions() throws InterruptedException
	{
		try
		{
		int numActions = 100;
		long userId = 1L;
		int actionType = 98;
		
		JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 25,Integer.MAX_VALUE,3,true,true,false);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		for(int i=0;i<numActions;i++)
		{
			Action a = new Action(0L,userId,2L,actionType,1,new Date(),0D,"clientUser"+userId,"clientItem2");
			actionQueue.put(a);
		}
		
		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		Collection<Action> actions = actionPeer.getRecentActions(numActions);
		
		Assert.assertEquals(0, actions.size());
		}
		finally
		{
		removeActions();
		}
	}
	
	@Test 
	public void testSingleThreadedNewUsersNoActionInserts() throws InterruptedException
	{
		try
		{
		int numActions = 100;
		long userId = 0L;
		String clientUserId = "user1";
		int actionType = 99;
		Random r = new Random();
		
		JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 25,Integer.MAX_VALUE,3,true,true,false);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		for(int i=0;i<numActions;i++)
		{
			Action a = new Action(0L,0L,1L,actionType,1,new Date(),0D,clientUserId+r.nextInt(),"1");
			actionQueue.put(a);
		}
		
		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		Collection<Action> actions = actionPeer.getRecentActions(numActions);
		
		Assert.assertEquals(0, actions.size());

		Collection<User> users = userPeer.getRecentUsers(1000);
		
		Assert.assertEquals(numActions, users.size());
		
		}
		finally
		{
		removeActions();
		}
	}
	
	
	@Test 
	public void testSingleThreadedNewItemsNoActionInserts() throws InterruptedException
	{
		try
		{
		int numActions = 100;
		long userId = 0L;
		String clientItemId = "item1-";
		String clientUserId = "user1";
		int actionType = 99;
		Random r = new Random();
		
		JdoAsyncActionQueue actionQueue = new JdoAsyncActionQueue(props.getClient(), 1, 25,Integer.MAX_VALUE,3,true,true,false);
		Thread queueRunner = new Thread(actionQueue);
		queueRunner.start();
		
		for(int i=0;i<numActions;i++)
		{
			Action a = new Action(0L,1L,0L,actionType,1,new Date(),0D,clientUserId,clientItemId+r.nextInt());
			actionQueue.put(a);
		}
		
		Thread.sleep(2000);
		
		actionQueue.setKeepRunning(false);
		queueRunner.join();
		
		Collection<Action> actions = actionPeer.getRecentActions(numActions);
		
		Assert.assertEquals(0, actions.size());

		Collection<Long> items = itemPeer.getRecentItemIds(0, numActions, new ConsumerBean(props.getClient()));
		
		Assert.assertEquals(numActions, items.size());
		
		}
		finally
		{
		removeActions();
		}
	}
	
}
