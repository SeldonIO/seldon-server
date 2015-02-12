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

package io.seldon.test.state;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.state.IHostState;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.state.jdo.SqlHostStatePeer;
import io.seldon.db.jdo.DatabaseException;

public class TestHostStatePeer extends BasePeerTest {

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
	
	@Test
	public void testActionUpdateNoEntry()
	{
		try
		{
			IHostState s = new SqlHostStatePeer();
			boolean actionUpdate = s.canRunActionUpdates("TEST");
			Assert.assertTrue(actionUpdate);
		}
		finally
		{
			clearData();
		}
	}
	
	@Test
	public void testActionUpdateFalseEntry()
	{
		try
		{
			addHostname("TEST", false);
			IHostState s = new SqlHostStatePeer();
			boolean actionUpdate = s.canRunActionUpdates("TEST");
			Assert.assertFalse(actionUpdate);
		}
		finally
		{
			clearData();
		}
	}
	
	@Test
	public void testActionUpdateTrueEntry()
	{
		try
		{
			addHostname("TEST", true);
			IHostState s = new SqlHostStatePeer();
			boolean actionUpdate = s.canRunActionUpdates("TEST");
			Assert.assertTrue(actionUpdate);
		}
		finally
		{
			clearData();
		}
	}

}
