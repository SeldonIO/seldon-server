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

package io.seldon.test.service.async;

import java.util.Properties;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.service.async.AsyncActionQueue;
import io.seldon.api.service.async.JdoAsyncActionQueue;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.service.async.JdoAsyncActionFactory;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;

public class TestJdoAsyncActionFactory extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	
	private void clearData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from hosts");
					query.execute();

				}
			});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
	
	
	private void addHostname(final String hostname,final boolean canRunActionUpdate)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into hosts (hostname,action_update) values (?,?)");
					query.execute(hostname,canRunActionUpdate);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to add hostname");
		}
		
	}
	
	private void updateHostname(final String hostname,final boolean canRunActionUpdate)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "update hosts set action_update=? where hostname=?");
					query.execute(canRunActionUpdate,hostname);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to update hostname");
		}
		
	}
	
	@Test
	public void testBasicPropsSetup()
	{
		try
		{
			Properties aprops = new Properties();
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".active", "true");
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".start", props.getClient());
			
			JdoAsyncActionFactory.create(aprops);
		
			AsyncActionQueue q = JdoAsyncActionFactory.get().get(props.getClient());
		
			Assert.assertNotNull(q);
		
			Assert.assertTrue(q.isRunUpdateIdsInActionTable());
		
			JdoAsyncActionQueue jq = (JdoAsyncActionQueue) q;
			
			Assert.assertTrue(jq.isInsertActions());
		
			
			q.setKeepRunning(false);
		}
		finally
		{
			JdoAsyncActionFactory.shutdown();
		}
	}
	
	@Test
	public void testPropsSayNoActionIdUpdate()
	{
		try
		{
			Properties aprops = new Properties();
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".active", "true");
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".start", props.getClient());
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+"."+props.getClient()+".update.ids", "false");
		
			JdoAsyncActionFactory.create(aprops);
		
			AsyncActionQueue q = JdoAsyncActionFactory.get().get(props.getClient());
		
			Assert.assertNotNull(q);
		
			Assert.assertFalse(q.isRunUpdateIdsInActionTable());
		
			q.setKeepRunning(false);
		}
		finally
		{
			JdoAsyncActionFactory.shutdown();
			clearData();
		}
	}
	
	@Test
	public void testUpdateActionsFlag() throws InterruptedException
	{
		try
		{

			Properties aprops = new Properties();
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".active", "true");
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".start", props.getClient());
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".timer.secs", "1");
			
			JdoAsyncActionFactory.create(aprops);
		
			AsyncActionQueue q = JdoAsyncActionFactory.get().get(props.getClient());
		
			Assert.assertNotNull(q);
		
			Assert.assertTrue(q.isRunUpdateIdsInActionTable());
		
			addHostname("TEST",false);
			
			Thread.sleep(4000);
			
			Assert.assertFalse(q.isRunUpdateIdsInActionTable());
			
			updateHostname("TEST",true);

			Thread.sleep(4000);
			
			Assert.assertTrue(q.isRunUpdateIdsInActionTable());

		}
		finally
		{
			JdoAsyncActionFactory.shutdown();
			clearData();
		}
	}
	
	
	@Test
	public void testNoActionInsertProps()
	{
		try
		{
			Properties aprops = new Properties();
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".active", "true");
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+".start", props.getClient());
			aprops.setProperty(JdoAsyncActionFactory.ASYNC_PROP_PREFIX+"."+props.getClient()+".insert.actions", "false");
			
			JdoAsyncActionFactory.create(aprops);
		
			AsyncActionQueue q = JdoAsyncActionFactory.get().get(props.getClient());
		
			Assert.assertNotNull(q);
		
			JdoAsyncActionQueue jq = (JdoAsyncActionQueue) q;
			
			Assert.assertFalse(jq.isInsertActions());
		
			q.setKeepRunning(false);
		}
		finally
		{
			JdoAsyncActionFactory.shutdown();
		}
	}

}
