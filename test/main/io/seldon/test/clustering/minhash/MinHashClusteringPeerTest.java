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

package io.seldon.test.clustering.minhash;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.minhash.MinHashClustering;
import io.seldon.clustering.minhash.jdo.MinHashSeedStorePeer;
import io.seldon.clustering.minhash.jdo.StoredClusterPeer;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.db.jdo.DatabaseException;

public class MinHashClusteringPeerTest extends BasePeerTest {

	@Autowired PersistenceManager pm;
	
	@Before
	public void setup()
	{
		MinHashClustering.initialise(true, 200, new MinHashSeedStorePeer(pm));
	}
	
	/**
	 * Cleanup method to remove hash seeds
	 */
	private void removeHashSeeds()
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from minhashseed");
					query.execute();
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
	private void removeUserHashes(final long userId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from minhashuser where user=?");
					query.execute(userId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
	
	@Test
	public void testMinHashFindMatch()
	{
		MinHashClustering mh = new MinHashClustering(3,1,new StoredClusterPeer(pm));
		Random r = new Random();
		
		Set<String> interests = new HashSet<>();
		for(int i=10;i<=10;i++)
			interests.add(""+i);
		
		long userId = r.nextLong();
		mh.storeHashesForUser(userId, interests);
		
		for(int i=10;i<11;i++)
			interests.add(""+i);
		
		mh = new MinHashClustering(3,1,new StoredClusterPeer(pm));
		long userId2 = r.nextLong();
		Map<Long,Integer> matches = mh.getSimilarUsers(userId2, interests, 20);

		Assert.assertEquals(1, matches.size());
		
		//Cleanup
		removeHashSeeds();
		removeUserHashes(userId);
	}
	
	@Test
	public void testMinHashNotFindMatch()
	{
		MinHashClustering mh = new MinHashClustering(3,1,new StoredClusterPeer(pm));
		Random r = new Random();
		
		Set<String> interests = new HashSet<>();
		for(int i=0;i<10;i++)
			interests.add(""+i);
		
		long userId = r.nextLong();
		mh.storeHashesForUser(userId, interests);
		
		interests.clear();
		for(int i=10;i<12;i++)
			interests.add(""+i);
		
		mh = new MinHashClustering(3,1,new StoredClusterPeer(pm));
		long userId2 = r.nextLong();
		Map<Long,Integer> matches = mh.getSimilarUsers(userId2, interests, 20);

		Assert.assertEquals(0, matches.size());
		
		//Cleanup
		removeHashSeeds();
		removeUserHashes(userId);
	}
	
}
