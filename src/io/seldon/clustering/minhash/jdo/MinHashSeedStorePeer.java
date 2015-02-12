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

package io.seldon.clustering.minhash.jdo;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.log4j.Logger;

import io.seldon.clustering.minhash.MinHashSeedStore;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;

public class MinHashSeedStorePeer implements MinHashSeedStore {

	private static Logger logger = Logger.getLogger(MinHashSeedStorePeer.class.getName());

	PersistenceManager pm;
	
	public MinHashSeedStorePeer(PersistenceManager pm)
	{
		this.pm = pm;
	}

	@Override
	public int getSeed(int i) {
		Query query = pm.newQuery( MinHashSeed.class, "position == i" );
		query.declareParameters( "java.lang.Integer i" );
		Collection<MinHashSeed> c = (Collection<MinHashSeed>) query.execute(i);
		if (c !=  null && c.size()==1)
			return c.iterator().next().getSeed();
		else
		{
			logger.warn("Can't find seed at position " + i);
			return -1;
		}
	}

	@Override
	public void storeSeed(final int i, final int seed) {
		try {
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	pm.makePersistent(new MinHashSeed(i,seed));
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to Add MinHashSeed " + seed,e);
		}
	}
	
	
	
}
