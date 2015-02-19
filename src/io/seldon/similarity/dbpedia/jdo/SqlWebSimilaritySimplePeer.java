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

package io.seldon.similarity.dbpedia.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jdo.Query;

import io.seldon.clustering.recommender.UserCluster;
import io.seldon.memcache.MemCacheKeys;
import org.apache.log4j.Logger;

import io.seldon.db.jdo.ClientPersistable;
import io.seldon.memcache.MemCachePeer;
import io.seldon.similarity.dbpedia.WebSimilaritySimpleStore;
import io.seldon.trust.impl.SharingRecommendation;
import io.seldon.util.CollectionTools;

public class SqlWebSimilaritySimplePeer extends ClientPersistable implements WebSimilaritySimpleStore {

private static final String USER_SIMILARITY_QUERY = 
		"select us.u2, us.score from user_similarity us LEFT JOIN interaction ui ON us.u1 = ui.u1 " + 
        "and ui.type = ? AND us.u2 = ui.u2 WHERE ui.u2 IS NULL AND us.u1=? and us.type=?";

private static final String JDO_SQL = "javax.jdo.query.SQL";

private static Logger logger = Logger.getLogger(SqlWebSimilaritySimplePeer.class.getName());
	
	private static Map<String,Set<Integer>> dimIdsMap = new ConcurrentHashMap<>();
	private static Map<String,Integer> clientToLikeActionType = new ConcurrentHashMap<>();
	
	public static void initialise(Properties props)
	{
		String clientStr = props.getProperty("io.seldon.semsim.clients");
		if (clientStr != null)
		{
			String clients[] = clientStr.split(",");
			for(int i=0;i<clients.length;i++)
			{
				String dimIdsStr = props.getProperty("io.seldon.semsim."+clients[i]+".dimids");
				if (dimIdsStr != null)
				{
					logger.info("Processing dim ids "+dimIdsStr+" for client "+clients[i]);
					Set<Integer> dimIds = new HashSet<>();
					String dimIdsStrParts[] = dimIdsStr.split(",");
					for(int j=0;j<dimIdsStrParts.length;j++)
					{
						dimIds.add(Integer.parseInt(dimIdsStrParts[j]));
					}
					dimIdsMap.put(clients[i], dimIds);
				}
				String actionTypeStr = props.getProperty("io.seldon.semsim."+clients[i]+".like.actiontype");
				if (actionTypeStr != null)
				{
					Integer actionType = Integer.parseInt(actionTypeStr);
					logger.info("Setting action type for likes for client "+clients[i]+" to "+actionType);
					clientToLikeActionType.put(clients[i], actionType);
				}

			}
		}
	}

	public SqlWebSimilaritySimplePeer(String client) {
		super(client);
	}
	
	private List<SharingRecommendation> getSharingRecommendation(Collection<Object[]> results)
	{
		List<SharingRecommendation> res = new ArrayList<>();
		Map<String,SharingRecommendation> userToShareRec = new HashMap<>();
		for(Object[] result : results)
		{
			Long userId = (Long) result[0]; // friend user id
			Long itemId1 = (Long) result[1]; // internal "article" item id
			Long itemId2 = (Long) result[2]; // external "like" item id
			String reason = (String) result[3]; // textual data for like
			String clientItemId = (String) result[4]; // external id for like
			String key = ""+userId+":"+itemId1;
			SharingRecommendation s = userToShareRec.get(key);
			if (s == null)
			{
				s = new SharingRecommendation(userId,itemId1);
				userToShareRec.put(key, s);
			}
			s.addItem(itemId2, clientItemId, reason, 1);
		}
		res.addAll(userToShareRec.values());
		if (res.size() > 0)
			Collections.sort(res,Collections.reverseOrder());
		return res;
	}

	/**
	 * Get the like action type for this client. Should have been set in initialise method.
	 * @return
	 */
	private int getLikeActionType()
	{
		Integer actionType = clientToLikeActionType.get(clientName);
		if (actionType != null)
			return actionType;
		else
		{
			logger.warn("Using default action type of 1");
			return 1;
		}
	}
	
	/**
	 *  Ignores linktype at present
	 */
	@Override
	public List<SharingRecommendation> getSharingRecommendations(
			List<Long> users, List<Long> items) {
		List<SharingRecommendation> res = new ArrayList<>();
		if (users != null && users.size() > 0 && items != null && items.size() > 0)
		{
			String userSet = CollectionTools.join(users, ",");
			String itemSet = CollectionTools.join(items,",");
			String sql = "select a.user_id,dit.item_id,a.item_id,tokens,a.client_item_id from dbpedia_item_tokens dit,ext_actions a,dbpedia_item_tokens dit2,dbpedia_token_token dtt, dbpedia_token_hits dth where dit2.item_id=a.item_id and dit.token_id=dtt.token_id1 and dit2.token_id=dtt.token_id2 and dit.item_id in ("+itemSet+") and a.user_id in ("+userSet+") and a.type=? and dth.token_id=dtt.token_id2 and dtt.good>dtt.bad";
			logger.info("SQL get user like matches is "+sql);
			Query query = getPM().newQuery( JDO_SQL, sql);
			Collection<Object[]> results = (Collection<Object[]>) query.execute(getLikeActionType());
			res = getSharingRecommendation(results);
			query.closeAll();
		}
		return res;
	}

	/**
	 *  Ignores linktype at present
	 */
	@Override
	public List<SharingRecommendation> getSharingRecommendationsForFriends(
			long userId, String linkType, List<Long> items) {
		List<SharingRecommendation> res = new ArrayList<>();
		if (items != null && items.size() > 0)
		{
			String itemSet = CollectionTools.join(items,",");
			String sql = "select a.user_id,dit.item_id,a.item_id,tokens,a.client_item_id from links l, link_type lt,dbpedia_item_tokens dit,ext_actions a,dbpedia_item_tokens dit2,dbpedia_token_token dtt, dbpedia_token_hits dth where lt.name=? and l.type=lt.type_id and l.u1=? and l.u2=a.user_id and dit2.item_id=a.item_id and dit.token_id=dtt.token_id1 and dit2.token_id=dtt.token_id2 and dit.item_id in ("+itemSet+") and a.type=? and dth.token_id=dtt.token_id2 and dtt.good>dtt.bad";
			logger.info("SQL get friends like matches is "+sql);
			Query query = getPM().newQuery( JDO_SQL, sql);
			long start = System.currentTimeMillis();
			Collection<Object[]> results = (Collection<Object[]>) query.execute(linkType,userId,getLikeActionType());
			logger.info("Time to run sharing sql: "+(System.currentTimeMillis()-start));
			res = getSharingRecommendation(results);
			query.closeAll();
		}
		return res;
	}

	static final int SEARCHED_EXPIRE_SECS = 3600;
	@Override
	public boolean hasBeenSearched(long itemId) {
		String key = MemCacheKeys.getDbpediaHasBeenSearched(this.clientName, itemId);
		Boolean searched = (Boolean) MemCachePeer.get(key);
		if (searched == null)
		{
			Query query = getPM().newQuery( JDO_SQL, "select item_id from dbpedia_item_user_searched where item_id=?");
			query.setUnique(true);
			query.setResultClass(Long.class);
			Long res = (Long) query.execute(itemId);
			if (res != null)
				searched = new Boolean(true);
			else
				searched = new Boolean(false);
			query.closeAll();
			MemCachePeer.put(key, searched, SEARCHED_EXPIRE_SECS);
		}
		return searched;
	}

	
	/**
	 *  Ignores linktype at present
	 */
	public List<SharingRecommendation> getSharingRecommendationsForFriendsFull(
			long userId, long itemId) {
		List<SharingRecommendation> res = new ArrayList<>();

		String sql = "select l.u2,dit.item_id,a.item_id,tokens,a.client_item_id from dbpedia_item_tokens dit join dbpedia_token_token dtt on (dit.item_id="+itemId+" and dit.token_id=dtt.token_id1 and dtt.good>dtt.bad) join dbpedia_item_tokens dit2 on (dit2.token_id=dtt.token_id2) join dbpedia_token_hits dth on (dth.token_id=dit2.token_id) join ext_actions a on (a.item_id=dit2.item_id and a.type="+getLikeActionType()+") join links l on (a.user_id=l.u2 and l.u1="+userId+")";
		logger.info("SQL get friends like matches is "+sql);
		Query query = getPM().newQuery( JDO_SQL, sql);
		long start = System.currentTimeMillis();	
		Collection<Object[]> results = (Collection<Object[]>) query.execute();
		logger.info("Time to run sharing sql: "+(System.currentTimeMillis()-start));
		res = getSharingRecommendation(results);
		query.closeAll();
		return res;
	}
	
	
	@Override
	public List<SharingRecommendation> getSharingRecommendationsForFriends(
			long userId, long itemId) {
		List<SharingRecommendation> res = new ArrayList<>();

		String sql = "select user_id,item_id,ex_item_id,tokens,ex_client_item_id from links join dbpedia_item_user diu where (u1="+userId+" and u2=user_id and item_id="+itemId+")";
		logger.info("SQL get friends like matches is "+sql);
		Query query = getPM().newQuery( JDO_SQL, sql);
		long start = System.currentTimeMillis();	
		Collection<Object[]> results = (Collection<Object[]>) query.execute();
		logger.info("Time to run sharing sql: "+(System.currentTimeMillis()-start));
		res = getSharingRecommendation(results);
		query.closeAll();
		return res;
	}
	
	private String getIdsSQL(Collection ids)
	{
		StringBuffer buf = new StringBuffer();
		boolean first = true;
		for(Object id : ids)
		{
			if (first)
				first = false;
			else
			{
				buf.append(",");
			}
			buf.append("?");
		}
		return buf.toString();
	}
	
	
	@Override
	public List<UserCluster> getUserDimClusters(long userid,
			Set<String> clientItemIds) {
		List<UserCluster> res = new ArrayList<>();
		if (clientItemIds != null &&  clientItemIds.size() > 0)
		{
			String itemSet = getIdsSQL(clientItemIds);
			String sql = "select dim_id as dimid,count(*) as support from (select d.dim_id,count(*) from item_attr_enum iae,item_map_enum e,dimension d,items i,items i2,dbpedia_item_tokens dit,dbpedia_item_tokens dit2,dbpedia_token_token dtt where dit2.item_id=i2.item_id and dit.token_id=dtt.token_id1 and dit2.token_id=dtt.token_id2 and i2.client_item_id in ("+itemSet+") and i.item_id=dit.item_id and i.type=1 and e.item_id=i.item_id and d.attr_id=e.attr_id and d.value_id=e.value_id and iae.attr_id=d.attr_id and iae.value_id=d.value_id group by dtt.token_id1,d.dim_id) t group by dim_id order by count(*) desc";
			logger.info("Running SQL: "+sql);
			Query query = getPM().newQuery( JDO_SQL, sql);
			query.setResultClass(UserDimClusterResult.class);
			Collection<UserDimClusterResult> results = (Collection<UserDimClusterResult>) query.executeWithArray(clientItemIds.toArray());
			if (results.size() > 0)
			{
				Set<Integer> filter = dimIdsMap.get(this.clientName);
				boolean first = true;
				int bestSupport  = 0;
				for(UserDimClusterResult r : results)
				{
					if (filter != null && filter.size() > 0 && !filter.contains(r.dimid))
					{
						logger.info("Ignoring result from dim id "+r.dimid+" as not in filter");
						continue;
					}
					else if (r.support <= 1)
					{
						logger.info("Skipping result from dim id "+r.dimid+" as support too low: support="+r.support);
						continue;
					}
					else
					{
						if (first)
						{
							bestSupport = r.support;
							first = false;
						}
						logger.info("Added cluster for dimId "+r.dimid+" with support "+r.support);
						res.add(new UserCluster(userid,r.dimid,0.5 * (r.support/(double)bestSupport),0L,0));						
					}

				}
			}
			query.closeAll();
		}
		logger.info("Returning "+res.size()+" clusters");
		for(UserCluster c : res)
			logger.info(c.toString());
			
		return res;

	}
	
	public static class UserDimClusterResult
	{
		int dimid;
		int support;
		
		public UserDimClusterResult()
		{
			
		}

		public UserDimClusterResult(int dimid, int support) {
			super();
			this.dimid = dimid;
			this.support = support;
		}

		public int getDimid() {
			return dimid;
		}

		public void setDimid(int dimid) {
			this.dimid = dimid;
		}

		public int getSupport() {
			return support;
		}

		public void setSupport(int support) {
			this.support = support;
		}
		
		
		
		
	}
	
	public static class ItemScore
	{
		long itemId;
		double score;
		
		public ItemScore() { }

		public ItemScore(long itemId, double score) {
			super();
			this.itemId = itemId;
			this.score = score;
		}

		public long getItemId() {
			return itemId;
		}

		public void setItemId(long itemId) {
			this.itemId = itemId;
		}

		public double getScore() {
			return score;
		}

		public void setScore(double score) {
			this.score = score;
		}
		
		
	}
	
	@Override
	public Map<Long, Double> getSocialPredictionRecommendations(long userId,int limit) {
		Map<Long,Double> res = new HashMap<>();

		String sql = "select item_id,sum(score) from dbpedia_item_user where user_id=? group by item_id order by item_id desc limit "+limit;
		logger.info("Running Social predict SQL: "+sql);
		Query query = getPM().newQuery( JDO_SQL, sql);
		query.setResultClass(ItemScore.class);
		Collection<ItemScore> qres = (Collection<ItemScore>) query.execute(userId);
		for(ItemScore i : qres)
			res.put(i.getItemId(), i.getScore());
		query.closeAll();
		
		return res;
	}

	@Override
	public Map<Long, Double> getSimilarUsers(long userId, int type, int interactionFilterType) {
		Map<Long,Double> similarUsers = new HashMap<>();
		Query query = getPM().newQuery( JDO_SQL, USER_SIMILARITY_QUERY);
		long start = System.currentTimeMillis();	
		Collection<Object[]> results = (Collection<Object[]>) query.execute(interactionFilterType, userId,type);
		logger.info("Time to run similar users sql: "+(System.currentTimeMillis()-start));
		for(Object[] r : results)
		{
			Long similarUserId = (Long) r[0];
			Double score = (Double) r[1];
			similarUsers.put(similarUserId, score);
		}
		return similarUsers;
	}

	
	private List<SharingRecommendation> getSharingRecommendationForKeywords(Collection<Object[]> results)
	{
		List<SharingRecommendation> res = new ArrayList<>();
		Map<String,SharingRecommendation> userToShareRec = new HashMap<>();
		for(Object[] result : results)
		{
			Long userId = (Long) result[0]; // friend user id
			String reason = (String) result[1]; // textual data for like
			String clientItemId = (String) result[2]; // external id for like
			Float score = (Float) result[3]; // score
			String key = ""+userId;
			SharingRecommendation s = userToShareRec.get(key);
			if (s == null)
			{
				s = new SharingRecommendation(userId,null);
				userToShareRec.put(key, s);
			}
			s.addItem(null, clientItemId, reason, score);
		}
		res.addAll(userToShareRec.values());
		if (res.size() > 0)
			Collections.sort(res,Collections.reverseOrder());
		return res;
	}
	
	@Override
	public List<SharingRecommendation> getSharingRecommendationsForFriends(
			long userid, List<String> tags) {
		if (tags != null && tags.size() > 0)
		{
			String sqlArgs = getIdsSQL(tags);
			String sql = "select u2,tokens,ex_client_item_id,score from dbpedia_keyword_user dku join dbpedia_keyword dk on (dk.k_id=dku.k_id and dku.u1=?) where dk.keyword in ("+sqlArgs+")";
			List<Object> args = new ArrayList<>();
			args.add(userid);
			for(String tag : tags)
				args.add(tag);
			Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
			Collection<Object[]> results = (Collection<Object[]>) query.executeWithArray(args.toArray());
			List<SharingRecommendation> res = getSharingRecommendationForKeywords(results);
			return res;
		}
		else
		{
			logger.error("empty tags passed in for user "+userid);
			return new ArrayList<>();
		}
	}

	
	
	
}
