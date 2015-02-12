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

package io.seldon.facebook;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;

import org.apache.log4j.Logger;

public class RummbleFacebookAccountsPeer {

	private static Logger logger = Logger.getLogger(RummbleFacebookAccountsPeer.class);
	private PersistenceManager pm;
	
	public RummbleFacebookAccountsPeer() {
		this.pm = JDOFactory.getPersistenceManager("facebook");
	}
	
	public RummbleFacebookAccountsPeer(PersistenceManager pm) {
		this.pm = pm;
	}
	
	public boolean setAccessToken(final RummbleFacebookAccount account, final Boolean active) {
		boolean res = true;
		try {
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					account.setTokenActive(active);
					pm.makePersistent(account);
				}
			});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to addAction",e);
			res=false;
		}
		
		return res;
		
	}
	
	public boolean setAccessToken(final RummbleFacebookAccount account, final Boolean active, final Long fbId) {
		boolean res = true;
		try {
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	account.setTokenActive(active);
			    	account.setFbId(fbId);
			    	pm.makePersistent(account);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to addAction",e);
			res=false;
		}
		
		return res;
		
	}
	
	public Collection<RummbleFacebookAccount> getRummbleFacebookAccounts(int limit) {
				
		Query query = pm.newQuery( RummbleFacebookAccount.class, "tokenActive == null" );
//		query.setOrdering("date desc,itemId desc");
		query.setRange(0, limit);
		Collection<RummbleFacebookAccount> c = (Collection<RummbleFacebookAccount>) query.execute();
		return c;

	}

    public Collection<RummbleFacebookAccount> getRummbleFacebookAccounts() {

		Query query = pm.newQuery( RummbleFacebookAccount.class, "tokenActive == null" );
		Collection<RummbleFacebookAccount> c = (Collection<RummbleFacebookAccount>) query.execute();
		return c;

	}
	
	public RummbleFacebookAccount getFacebookAccount(Long internalID) {
				
		RummbleFacebookAccount a = pm.getObjectById(RummbleFacebookAccount.class, internalID);
		return a;
		
	}
	

}
