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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.Transaction;
import org.apache.log4j.Logger;

import io.seldon.clustering.minhash.StoredClusterHandler;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.TransactionPeer;

public class StoredClusterPeer implements StoredClusterHandler {

	private static Logger logger = Logger.getLogger(StoredClusterPeer.class.getName());

	PersistenceManager pm;
	
	public StoredClusterPeer(PersistenceManager pm)
	{
		this.pm = pm;
	}
	
	@Override
	public Map<Long, Integer> getUsersNonHash(Set<String> clientItemIds,int max) {
		if (clientItemIds.size() == 0)
			return new HashMap<>();
		StringBuffer sql = new StringBuffer("select u.user_id,count(*) from actions a, items i, users u where i.item_id=a.item_id and i.item_id in (");
		boolean first = true;
		for(String id : clientItemIds)
		{
			if (first)
				first = false;
			else
				sql.append(",");
			sql.append("\"");
			sql.append(id);
			sql.append("\"");
		}
		sql.append(") and u.user_id=a.user_id and u.active group by user_id order by count(*) desc limit "+max);
		String sqlStr = sql.toString();
		logger.info(sqlStr);
		Query query = pm.newQuery( "javax.jdo.query.SQL",sqlStr);
		List<Object[]> users = (List<Object[]>) query.execute();
		Map<Long,Integer> res = new HashMap<>();
		for(Object[] row : users)
			res.put((Long)row[0], ((Long)row[1]).intValue());
		return res;
	}
	
	public Set<Long> getUsersJDOQL(String hash) {
		Query query = pm.newQuery( UserMinHash.class, "hash.startsWith(h)" );
		query.declareParameters( "java.lang.String h" );
		Collection<UserMinHash> c = (Collection<UserMinHash>) query.execute(hash);
		if (c != null)
		{
			Set<Long> users = new HashSet<>();
			for(UserMinHash u : c)
				users.add(u.getUser());
			return users;
		}
		else
			return null;
	}
	
	@Override
	public Set<Long> getUsers(String hash) {
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select user from minhashuser where hash like \""+hash+"%\"" );
		Collection<Long> users = (Collection<Long>) query.execute();
		Set<Long> res = new HashSet<>();
		for(Long user : users)
			res.add(user);
		return res;
	}
	
	@Override
	public Set<Long> getUsers(Set<String> hashes) {
		if (hashes.size() == 0)
		{
			return new HashSet<>();
		}
		else
		{
			StringBuffer sql = new StringBuffer("select user from minhashuser where (");
			boolean first = true;
			for(String hash : hashes)
			{
				if (first)
					first = false;
				else
					sql.append(" or ");
				sql.append("hash like \"");
				sql.append(hash);
				sql.append("%\"");
			}
			sql.append(") group by user");
			logger.info(sql.toString());
			Query query = pm.newQuery( "javax.jdo.query.SQL",sql.toString());
			Collection<Long> users = (Collection<Long>) query.execute();
			Set<Long> res = new HashSet<>();
			for(Long user : users)
				res.add(user);
			return res;
		}
	}

	@Override
	public void storeHashes(final long user, final Set<String> hashes) {
		try {
			TransactionPeer.runTransaction(new Transaction(pm) {
			    public void process()
			    { 
			    	for(String hash : hashes)
			    	{
			    		pm.makePersistent(new UserMinHash(hash,user));
			    	}
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to Add UserMinHahses for user " + user,e);
		}
	}

	@Override
	public boolean hashesAvailable() {
		Extent<UserMinHash> extent = pm.getExtent(UserMinHash.class);
		if (extent.iterator().hasNext())
			return true;
		else
			return false;
	}

	

	

	

}
