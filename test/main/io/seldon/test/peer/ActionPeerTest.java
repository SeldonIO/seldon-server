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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.Action;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ActionPeerTest  extends BasePeerTest {

	@Autowired PersistenceManager pm;
	
	private void addAction(final long userId,final long itemId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "insert into actions values (0,?,?,99,1,now(),0,?,?)");
					List<Object> args = new ArrayList<Object>();
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
	
	private void removeActions()
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from actions");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear actions");
		}
	}
	
	@Test
	public void getRecentItemIdsTest()
	{
		Random r = new Random();
		for(int i=0;i<100;i++)
			addAction(r.nextLong(),r.nextLong());
		
		Collection<Action> actions = actionPeer.getRecentActions(1);
		Assert.assertTrue(actions.size() > 0);
		Action action = actions.iterator().next();
		List<Long> recentActions = actionPeer.getRecentUserActions(action.getUserId());
		Assert.assertTrue(recentActions.size() >= 1);
		Assert.assertEquals((Long) action.getItemId(), (Long) recentActions.get(0));
	}
}
