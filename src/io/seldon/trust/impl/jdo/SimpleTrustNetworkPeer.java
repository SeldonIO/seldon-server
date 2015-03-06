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

package io.seldon.trust.impl.jdo;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.recommendation.RecommendationNetworkImpl;
import org.apache.log4j.Logger;

import io.seldon.api.Constants;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.trust.impl.RecommendationNetwork;
import io.seldon.trust.impl.TrustNetworkSupplier.CF_TYPE;

public class SimpleTrustNetworkPeer {
	private static Logger logger = Logger.getLogger(SimpleTrustNetworkPeer.class.getName());
	private PersistenceManager pm;

	public SimpleTrustNetworkPeer(PersistenceManager pm) {
		this.pm = pm;
	}
	
	public RecommendationNetwork getNetwork(long user,int type,CF_TYPE trustType)
	{
		RecommendationNetworkImpl net = new RecommendationNetworkImpl(user,type);
		Map<Long,Double> trusts = new HashMap<>();
		String sql;
		if (trustType == CF_TYPE.USER)
			sql = "select u2,trust from network where u1=? && type=?";
		else
			sql = "select c2,trust from network_content where c1=? && type=?";
		
		Query query = pm.newQuery("javax.jdo.query.SQL",sql);
		List<Object[]> res = (List<Object[]>) query.execute(user,type);
		for(Object[] r : res)
		{
			trusts.put((Long)r[0], (Double)r[1]);
		}
		net.setTrust(trusts);
		return net;
	}
	
	public void addTrust(final long u1,final int type,final long u2,final double trust)
	{
		try {
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "insert ignore into network values (" + u1 + "," + type + "," + u2 + "," + trust + ")");
					query.execute();
				}
			});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to Add trust link  " + u1 + ","+u2+","+trust,e);
		}
	}

	public RecommendationNetwork getCooccurrenceNetwork(long user) {
		RecommendationNetworkImpl net = new RecommendationNetworkImpl(user,Constants.DEFAULT_DIMENSION); 
		Map<Long,Double> trusts = new HashMap<>();
		String sql;
		sql = "select user_id,cast(c as decimal(5,5)) from (select u.user_id,1-least(0.6,(1/count(*))) c from actions a1 inner join actions a2 on a1.user_id=? and a1.item_id=a2.item_id and a1.user_id<>a2.user_id inner join users u on u.active and a2.user_id=u.user_id group by u.user_id order by count(*) desc limit 100) a;";
		Query query = pm.newQuery("javax.jdo.query.SQL",sql);
		List<Object[]> res = (List<Object[]>) query.execute(user);
		for(Object[] r : res)
		{
			Double d = ((BigDecimal)r[1]).doubleValue();
			trusts.put((Long)r[0], d);
		}
		net.setTrust(trusts);
		return net;
	}

	public RecommendationNetwork getJaccardUpdatedNetwork(long userId, Set<Long> users, Long actions) {
		RecommendationNetworkImpl net = new RecommendationNetworkImpl(userId,Constants.DEFAULT_DIMENSION); 
		Map<Long,Double> trusts = new HashMap<>();
		//SQL
		String sql;
  		String usersIds ="";
  		for(Long l: users) { usersIds += l + ","; }
  		if(usersIds.length()>0) { usersIds = usersIds.substring(0, usersIds.length()-1); }
  		sql = "select a2.user_id,count(*)/(u1.num_op+u2.num_op-count(*)) + 0.5  from actions a1 inner join actions a2 on a1.item_id=a2.item_id and a1.user_id=? inner join users u1 on a1.user_id=u1.user_id inner join users u2 on a2.user_id=u2.user_id  where a2.user_id in ("+ usersIds +") group by a2.user_id;";
		if(actions != null) { sql = sql.replace("u1.num_op",actions.toString()); }
  		Query query = pm.newQuery("javax.jdo.query.SQL",sql);
		List<Object[]> res = (List<Object[]>) query.execute(userId);
		for(Object[] r : res)
		{
			Double d = ((BigDecimal)r[1]).doubleValue();
			trusts.put((Long)r[0], d);
		}
		net.setTrust(trusts);
		return net;
	}
}
