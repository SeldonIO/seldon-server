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

package io.seldon.general.jdo;

import java.util.Collection;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.log4j.Logger;

import io.seldon.general.Opinion;
import io.seldon.general.OpinionPeer;

public class SqlOpinionPeer extends OpinionPeer {

	private static Logger logger = Logger.getLogger(SqlOpinionPeer.class.getName());
	private PersistenceManager pm;

	public SqlOpinionPeer(PersistenceManager pm) {
		this.pm = pm;
	}
	
	public Collection<Opinion> getRecentOpinions(int limit)
	{
		Query query = pm.newQuery( Opinion.class, "" );
		query.setOrdering("time desc,itemId desc");
		query.setRange(0, limit);
		Collection<Opinion> c = (Collection<Opinion>) query.execute();
		return c;
	}
	
	public Collection<Opinion> getUserOpinions(long userId,int limit)
	{
		Query query = pm.newQuery( Opinion.class, "userId == u" );
		query.declareParameters( "java.lang.Long u" );
		query.setOrdering("time desc,itemId desc");
		query.setRange(0, limit);
		Collection<Opinion> c = (Collection<Opinion>) query.execute(userId);
		return c;
	}
	
	public Collection<Opinion> getItemOpinions(long itemId,int limit)
	{
		Query query = pm.newQuery( Opinion.class, "itemId == i" );
		query.declareParameters( "java.lang.Long i" );
		query.setOrdering("time desc,userId desc");
		query.setRange(0, limit);
		Collection<Opinion> c = (Collection<Opinion>) query.execute(itemId);
		return c;
	}
	
	public Opinion getOpinion(long itemId, long userId) {
		Opinion o = null;
		Query query = pm.newQuery( Opinion.class, "itemId == i && userId == u" );
		query.declareParameters( "java.lang.Long i,java.lang.Long u" );
		Collection<Opinion> c = (Collection<Opinion>) query.execute(itemId,userId);
		if(!c.isEmpty()) {
			o = c.iterator().next();
		}
		return o;
	}

	@Override
	public long getNumSharedOpinions(long userId1, long userId2) {
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select count(*) from (select item_id from opinions where user_id=?) a1 join (select item_id from opinions where user_id=?) a2 on (a1.item_id=a2.item_id)" );
		query.setUnique(true);
		Long ans = (Long) query.execute(userId1, userId2);
		if (ans != null)
			return ans;
		else
		{
			logger.warn("Failed to getNumSharedOpinions for " + userId1 + " and " + userId2);
			return 0;
		}
	}

	@Override
	public long getNumOpinions(long userId) {
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select count(*) from opinions where user_id=?" );
		query.setUnique(true);
		Long ans = (Long) query.execute(userId);
		if (ans != null)
			return ans;
		else
		{
			logger.warn("Failed to getNumOpinions for " + userId);
			return 0;
		}
	}

	@Override
	public long getNumItemOpinions(long itemId) {
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select count(*) from opinions where item_id=?" );
		query.setUnique(true);
		Long ans = (Long) query.execute(itemId);
		if (ans != null)
			return ans;
		else
		{
			logger.warn("Failed to getNumItempinions for " + itemId);
			return 0;
		}
	}
}
