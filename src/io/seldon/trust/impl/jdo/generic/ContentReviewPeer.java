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

package io.seldon.trust.impl.jdo.generic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.prediction.ContentRatingResolver;
import io.seldon.trust.impl.Trust;
import io.seldon.trust.impl.TrustNetwork;
import org.apache.log4j.Logger;

import io.seldon.api.Constants;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.general.ItemType;
import io.seldon.general.Opinion;


public class ContentReviewPeer implements ContentRatingResolver {

	private static Logger logger = Logger.getLogger(ContentReviewPeer.class.getName());
	
	PersistenceManager pm;
	ResolveStrategy strategy;
	
	public ContentReviewPeer(PersistenceManager pm)
	{
		this.pm = pm;
	}
	
	public Collection<Opinion> getContentReviews()
	{
		Extent<Opinion> e = pm.getExtent(Opinion.class);
		ArrayList<Opinion> a = new ArrayList<Opinion>();
		for(Iterator<Opinion> i = e.iterator();i.hasNext();)
			a.add(i.next());
		e.closeAll();
		return a;
	}
	 
	public Collection<Long> getContent(long src,int type,double minTrust,double minRating,int max)
	{
		//FIXME - handle type
		if (type == Trust.TYPE_GENERAL)
		{
			Query query = pm.newQuery("javax.jdo.query.SQL", "select item_id from opinions o,trust_network t where t.srcuser=? and t.dstuser=o.user_id and b+u*a>"+minTrust+" and o.value>"+minRating+" order by value desc limit " + max);
			Collection<Long> c = (Collection<Long>) query.execute(src);
			return c;
		}
		else
			return new HashSet<Long>();
	}
	
	public Collection<Long> getContentTrustNet(TrustNetwork trustNet,long src,int type,double minTrust,double minRating,int max)
	{
		//FIXME - handle type
		if (type == Trust.TYPE_GENERAL)
		{
			String set = getSQLSet(trustNet.getMembers());
			String sql = "select item_id from opinions o where o.user_id in "+set+" and o.value>="+minRating+" and not exists (select * from opinions where user_id="+src+" and item_id=o.item_id) group by item_id order by o.time desc limit " + max;
			logger.info("get trusted content " + sql);
			Query query = pm.newQuery("javax.jdo.query.SQL", sql);
			Collection<Long> c = (Collection<Long>) query.execute(src);
			return c;
		}
		else
			return new HashSet<Long>();
	}
	
	private String getSQLSet(Set<Long> ids)
	{
		StringBuffer buf = new StringBuffer("(");
		for(Long id : ids)
		{
			buf.append(id);
			buf.append(",");
		}
		buf.deleteCharAt(buf.length()-1);
		buf.append(")");
		return buf.toString();
	}
	
	public Collection<Long> getPopularTrustedContentTrustNet(Set<Long> neighbours,long src,int type,double minRating,int max)
	{
		if (type == Trust.TYPE_GENERAL)
		{
			String set = getSQLSet(neighbours);
			String sql = "select item_id from (select item_id,count(*) as c from opinions o where o.user_id in "+set+" and o.value>="+minRating+" and not exists (select * from opinions where user_id="+src+" and item_id=o.item_id) group by item_id order by o.time limit 1000) t order by c desc limit "+max;
			logger.info("Get popular recs: " + sql);
			Query query = pm.newQuery("javax.jdo.query.SQL",sql); 
			Collection<Long> c = (Collection<Long>) query.execute(src);
			return c;
		}
		else
		{
		
			try {
				String set = getSQLSet(neighbours);
				String sql = "select item_id from (select o.item_id,count(*) as c from opinions o natural join item_map_enum m where o.user_id in "+set+" and o.value>="+minRating+" and m.attr_id=? and m.value_id=? and not exists (select * from opinions where user_id="+src+" and item_id=o.item_id) group by item_id order by o.time limit 1000) t order by c desc limit "+max;
				Integer[] attr = Util.getItemPeer(pm).getAttributes(type);
				logger.info("Get popular recs: " + sql);
				Query query = pm.newQuery("javax.jdo.query.SQL",sql); 
				Collection<Long> c = (Collection<Long>) query.execute(src,attr[0],attr[1]);
				return c;
			}
			catch(Exception e) {
				return null;
			}
		}
	}
	
	
	public Collection<Long> getPopularTrustedContent(long src,Integer type, int dimension,int k,int numRecommendations)
	{
		//check the item type
		//if the item type is internal o external
		//if it is internal use just active users
		//TODO change to use the ItemService (Cached Bean)
		boolean internalItem = true;
		if(type!=null) {
			ItemType itemType = Util.getItemPeer(pm).getItemType(type);
			if(itemType != null && itemType.getLinkType() != null) { internalItem = false; }
		}
		//WITH A SPECIFIC ITEM TYPE
		if(type!=null) {
			
			//USING DIMENSIONS
			if (dimension == Constants.DEFAULT_DIMENSION)
			{
				String sql;
				if(internalItem) {
					sql = "select a.item_id from (select item_id,sum(trust) t from (select o.item_id,trust from (select u2 as user_id,trust from network n inner join users u on n.u2=u.user_id and u.active  where u1=? order by trust desc limit ?) a1 natural join opinions o inner join items on o.item_id=items.item_id and items.type=" + type +" left join (select item_id from opinions where user_id=?) a2 on o.item_id=a2.item_id where a2.item_id is null) a group by item_id) a left join item_map_enum m on a.item_id=m.item_id left join dimension d on m.attr_id=d.attr_id and m.value_id=d.value_id left join user_dim ud on ud.user_id=? and d.dim_id=ud.dim_id group by a.item_id order by t*coalesce(1 + sum(relevance),1) desc,item_id desc limit ?";
				}
				else {
					sql = "select a.item_id from (select item_id,sum(trust) t from (select o.item_id,trust from (select u2 as user_id,trust from network where u1=? order by trust desc limit ?) a1 natural join opinions o inner join items on o.item_id=items.item_id and items.type=" + type +" left join (select item_id from opinions where user_id=?) a2 on o.item_id=a2.item_id where a2.item_id is null) a group by item_id) a left join item_map_enum m on a.item_id=m.item_id left join dimension d on m.attr_id=d.attr_id and m.value_id=d.value_id left join user_dim ud on ud.user_id=? and d.dim_id=ud.dim_id group by a.item_id order by t*coalesce(1 + sum(relevance),1) desc,item_id desc limit ?";
				}
				Query query = pm.newQuery("javax.jdo.query.SQL", sql);
				ArrayList<Object> args = new ArrayList<Object>();
				args.add(src);
				args.add(k);
				args.add(src);
				args.add(src);
				args.add(numRecommendations);
				Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
				return c;
			}
			//NO DIMENSIONS
			else if(dimension == Constants.NO_TRUST_DIMENSION)
			{
				Query query = pm.newQuery("javax.jdo.query.SQL", "select item_id from (select o.item_id,trust from opinions o inner join items on o.item_id=items.item_id and items.type=" + type +" natural join (select u2 as user_id,trust from network where u1=? order by trust desc limit ?) a1 left join	(select item_id from opinions where user_id=?) a2 on o.item_id=a2.item_id where a2.item_id is null) a group by item_id order by sum(trust) desc limit ?");
				ArrayList<Object> args = new ArrayList<Object>();
				args.add(src);
				args.add(k);
				args.add(src);
				args.add(numRecommendations);
				Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
				return c;
			}
			//ONE DIMENSION
			else
			{
				//SPECIFIC CATEGORY
				Query query = pm.newQuery("javax.jdo.query.SQL", "select a.item_id from (select item_id,sum(trust) t from (select o.item_id,trust from (select u2 as user_id,trust from network where u1=? order by trust desc limit ?) a1 natural join opinions o inner join items on o.item_id=items.item_id and items.type=" + type +" INNER JOIN item_map_enum M ON o.item_id=M.item_id INNER JOIN dimension d ON M.attr_id = d.attr_id AND M.value_id=d.value_id AND d.dim_id=? left join	(select item_id from opinions where user_id=?) a2 on o.item_id=a2.item_id where a2.item_id is null) a group by item_id) a left join item_map_enum m on a.item_id=m.item_id left join dimension d on m.attr_id=d.attr_id and m.value_id=d.value_id left join user_dim ud on ud.user_id=? and d.dim_id=ud.dim_id group by a.item_id order by t*coalesce(1 + sum(relevance),1) desc,item_id desc limit ?");
				ArrayList<Object> args = new ArrayList<Object>();
				args.add(src);
				args.add(k);
				args.add(dimension);
				args.add(src);
				args.add(src);
				args.add(numRecommendations);
				Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
				return c;
			}
		}
		//WITH NO SPECIFIC ITEM TYPE
		else {
			//USING DIMENSIONS
			if (dimension == Constants.DEFAULT_DIMENSION)
			{
				Query query = pm.newQuery("javax.jdo.query.SQL", "select a.item_id from (select item_id,sum(trust) t from (select o.item_id,trust from (select u2 as user_id,trust from network where u1=? order by trust desc limit ?) a1 natural join opinions o left join	(select item_id from opinions where user_id=?) a2 on o.item_id=a2.item_id where a2.item_id is null) a group by item_id) a left join item_map_enum m on a.item_id=m.item_id left join dimension d on m.attr_id=d.attr_id and m.value_id=d.value_id left join user_dim ud on ud.user_id=? and d.dim_id=ud.dim_id group by a.item_id order by t*coalesce(1 + sum(relevance),1) desc,item_id desc limit ?");
				ArrayList<Object> args = new ArrayList<Object>();
				args.add(src);
				args.add(k);
				args.add(src);
				args.add(src);
				args.add(numRecommendations);
				Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
				return c;
			}
			//NO DIMENSIONS
			else if(dimension == Constants.NO_TRUST_DIMENSION)
			{
				Query query = pm.newQuery("javax.jdo.query.SQL", "select item_id from (select o.item_id,trust from opinions o natural join (select u2 as user_id,trust from network where u1=? order by trust desc limit ?) a1 left join	(select item_id from opinions where user_id=?) a2 on o.item_id=a2.item_id where a2.item_id is null) a group by item_id order by sum(trust) desc limit ?");
				ArrayList<Object> args = new ArrayList<Object>();
				args.add(src);
				args.add(k);
				args.add(src);
				args.add(numRecommendations);
				Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
				return c;
			}
			//ONE DIMENSION
			else
			{
				//SPECIFIC CATEGORY
				Query query = pm.newQuery("javax.jdo.query.SQL", "select a.item_id from (select item_id,sum(trust) t from (select o.item_id,trust from (select u2 as user_id,trust from network where u1=? order by trust desc limit ?) a1 natural join opinions o INNER JOIN item_map_enum M ON o.item_id=M.item_id INNER JOIN dimension d ON M.attr_id = d.attr_id AND M.value_id=d.value_id AND d.dim_id=? left join	(select item_id from opinions where user_id=?) a2 on o.item_id=a2.item_id where a2.item_id is null) a group by item_id) a left join item_map_enum m on a.item_id=m.item_id left join dimension d on m.attr_id=d.attr_id and m.value_id=d.value_id left join user_dim ud on ud.user_id=? and d.dim_id=ud.dim_id group by a.item_id order by t*coalesce(1 + sum(relevance),1) desc,item_id desc limit ?");
				ArrayList<Object> args = new ArrayList<Object>();
				args.add(src);
				args.add(k);
				args.add(dimension);
				args.add(src);
				args.add(src);
				args.add(numRecommendations);
				Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
				return c;
			}
		}
	}
	
	public Collection<Long> getPopularTrustedContent(long src,int type,double minTrust,double minRating,int max)
	{
		//FIXME - handle type
		if (type == Trust.TYPE_GENERAL)
		{
			Query query = pm.newQuery("javax.jdo.query.SQL", "select item_id from (select item_id,avg(b+u*a) as t,count(*) as c from opinions o,trust_network t where t.srcuser=? and t.dstuser=o.user_id and b+u*a>+"+minTrust+" and o.value>"+minRating+" group by item_id order by o.time limit 1000) t order by c desc limit 100");
			Collection<Long> c = (Collection<Long>) query.execute(src);
			return c;
		}
		else
			return new HashSet<Long>();
	}
	
	
	public Collection<Long> getMostPopularContent(long src,int dimension,int numRecommendations)
	{
		//FIXME - needs to use user_dim to get a selection of most popular based on user's tastes
		if (dimension == Constants.DEFAULT_DIMENSION)
		{
			Query query = pm.newQuery("javax.jdo.query.SQL", "select p.item_id from (select * from items_popular limit 1000) p left join item_map_enum m on p.item_id=m.item_id inner join dimension d on m.attr_id=d.attr_id and m.value_id=d.value_id left join user_dim ud on ud.user_id=? and d.dim_id=ud.dim_id left join (select item_id from opinions where user_id=?) a2 on p.item_id=a2.item_id where a2.item_id is null group by p.item_id order by opsum*coalesce(1 + sum(relevance),1) desc,item_id desc limit ?");
			ArrayList<Object> args = new ArrayList<Object>();
			args.add(src);
			args.add(src);
			args.add(numRecommendations);
			Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
			return c;
		}
		//NO DIMENSIONS
		else if(dimension == Constants.NO_TRUST_DIMENSION)
		{
			Query query = pm.newQuery("javax.jdo.query.SQL", "select p.item_id from items_popular p left join  (select item_id from opinions where user_id=?) a on p.item_id=a.item_id where a.item_id is null order by opsum desc,item_id desc limit ?");
			ArrayList<Object> args = new ArrayList<Object>();
			args.add(src);
			args.add(numRecommendations);
			Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
			return c;
		}
		//ONE DIMENSION
		else
		{
			Query query = pm.newQuery("javax.jdo.query.SQL", "select p.item_id from items_popular p natural join item_map_enum m natural join dimension d left join (select item_id from opinions where user_id=?) a on p.item_id=a.item_id  where a.item_id is null and d.dim_id=? order by opsum desc,item_id desc limit ?");
			ArrayList<Object> args = new ArrayList<Object>();
			args.add(src);
			args.add(dimension);
			args.add(numRecommendations);
			Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
			return c;
		}

	}
	
	
	public void removeAll() {
		Collection<Opinion> c = getContentReviews();
		for(Opinion r : c) { pm.deletePersistent(r); }
	}
	
	public Collection<Long> getContentReviewers(long contentId)
	{
		ArrayList<Long> reviewers = new ArrayList<Long>();
		Collection<Opinion> c = getContent(contentId);
		for(Opinion r : c)
			reviewers.add(r.getUserId());
		return reviewers;
	}
	
	public Opinion getContent(long contentId,long userId)
	{
		Query query = pm.newQuery( Opinion.class, "itemId == c && userId == u" );
		query.declareParameters( "java.lang.Long c,java.lang.Long u" );
		Collection<Opinion> c = (Collection<Opinion>) query.execute(contentId,userId);
		if(c!=null && c.size()>0) {
			return c.iterator().next();
		}
		else { return null; }
	}
	
	public Collection<Opinion> getContent(long contentId)
	{
		Query query = pm.newQuery( Opinion.class, "itemId == c" );
		query.declareParameters( "java.lang.Long c" );
		Collection<Opinion> c = (Collection<Opinion>) query.execute(contentId);
		return c;
	}
	
	public Collection<Opinion> getRecentContent(long contentId,int limit)
	{
		Query query = pm.newQuery( Opinion.class, "itemId == c" );
		query.declareParameters( "java.lang.Long c" );
		query.setOrdering("time desc");
		query.setRange(0, limit);
		Collection<Opinion> c = (Collection<Opinion>) query.execute(contentId);
		return c;
	}
	
	public Collection<Opinion> getRecentUserContent(long userId,int limit)
	{
		Query query = pm.newQuery( Opinion.class, "userId == u" );
		query.declareParameters( "java.lang.Long u" );
		query.setOrdering("time desc");
		query.setRange(0, limit);
		Collection<Opinion> c = (Collection<Opinion>) query.execute(userId);
		return c;
	}
	
	public Collection<Opinion> getContentForUser(long userId)
	{
		Query query = pm.newQuery( Opinion.class, "userId == u" );
		query.declareParameters( "java.lang.Long u" );
		Collection<Opinion> c = (Collection<Opinion>) query.execute(userId);
		return c;
	}
	
	public Collection<Long> getContentIdsForUser(long userId)
	{
		Collection<Opinion> c = getContentForUser(userId);
		Set<Long> cids = new HashSet<Long>();
		for(Iterator<Opinion> i = c.iterator();i.hasNext();)
			cids.add(i.next().getItemId());
		return cids;
	}
	
	
	
	public void addContent(long itemId,long userId,double value,Date date)
	{
		if (date == null) { date = new Date(); }
		Opinion o = new Opinion(userId,itemId,value,date);
		pm.makePersistent(o);
	}
	
	

	// how to handle multiple ratings in a generic way as used by personal rating generator?
	// get last rating?
	// get average rating... have that as sort option passed in?
	@Override
	public Double getRating(long userId, long itemId) 
	{
		Opinion o = null;
		try {
			o = Util.getOpinionPeer(pm).getOpinion(itemId, userId);
		} catch (APIException e) { }
		if (o != null)	{
			if (strategy == null || strategy == ContentRatingResolver.ResolveStrategy.LATEST)
				return o.getValue();
			else if (strategy == ContentRatingResolver.ResolveStrategy.AVERAGE)
			{
				return o.getValue();
			}
			else {
				return o.getValue();
			}
		}
		else
			return null;
	}

	@Override
	public void setResolveStrategy(ResolveStrategy strategy) {
		this.strategy = strategy;
	}

	@Override
	public Double getAvgRating(long m, int type) {
		try {
			return Util.getUserPeer(pm).getUserAvgRating(m, type);
		} catch (APIException e) {
			return Constants.DEFAULT_OPINION_VALUE;
		}
	}

	private Double getAvgRatingImpl(final long m1, final long m2, int type) {
		Double avg = null;
		if (m1 < m2)
		{
			Query query = pm.newQuery("javax.jdo.query.SQL","select avgm1 from user_user_avg a where a.m1=? and a.m2=?");
			query.setUnique(true);
			avg = (Double) query.execute(m1, m2);
		}
		else
		{
			Query query = pm.newQuery("javax.jdo.query.SQL","select avgm2 from user_user_avg a where a.m1=? and a.m2=?");
			query.setUnique(true);
			avg = (Double) query.execute(m2, m1);
		}
		return avg;
	}
	
	@Override
	public Double getAvgRating(final long m1, final long m2, int type) {
		//FIXME ignores type
		Double avg = getAvgRatingImpl(m1,m2,type);
		// work out from scratch
		if (avg == null)
		{
			try {
				TransactionPeer.runTransaction(new Transaction(pm) {
					public void process() {
						updateUserUserAvg(m1, m2);
					}
				});
			} catch (DatabaseException e) {
				logger.error("Failed to update user-user avg",e);
				return null;
			}
			
			return getAvgRating(m1,m2,type);
		}
		else
			return avg;
	}
	
	public void updateUserUserAvg(long m1,long m2)
	{
		Query query = pm.newQuery("javax.jdo.query.SQL","insert into user_user_avg  select t1.m1,t1.m2,t1.avgm1,t2.avgm2 from (select o1.user_id as m1,o2.user_id as m2,avg(o1.value) as avgm1  from opinions o1,opinions o2 where o1.user_id=? and o2.user_id=? and o1.item_id=o2.item_id group by o1.user_id,o2.user_id) t1, (select o1.user_id as m1,o2.user_id as m2,avg(o2.value) as avgm2  from opinions o1,opinions o2 where o1.user_id=? and o2.user_id=? and o1.item_id=o2.item_id group by o1.user_id,o2.user_id) t2 on duplicate key update avgm1=t1.avgm1,avgm2=t2.avgm2");
		ArrayList<Object> args = new ArrayList<Object>();
		args.add(m1 < m2 ? m1 : m2);
		args.add(m1 < m2 ? m2 : m1);
		args.add(m1 < m2 ? m1 : m2);
		args.add(m1 < m2 ? m2 : m1);
		query.executeWithArray(args.toArray());
		query.closeAll();
		Double avg = getAvgRatingImpl(m1,m2,Trust.TYPE_GENERAL);
		if (avg == null)
		{
			query = pm.newQuery("javax.jdo.query.SQL","insert into user_user_avg  select t1.m1,t2.m2,t1.avgm1,t2.avgm2 from (select user_id as m1,avgrating as avgm1 from users where user_id=?) t1, (select user_id as m2,avgrating as avgm2 from users where user_id=?) t2 on duplicate key update avgm1=t1.avgm1,avgm2=t2.avgm2");
			args = new ArrayList<Object>();
			args.add(m1 < m2 ? m1 : m2);
			args.add(m1 < m2 ? m2 : m1);
			args.add(m1 < m2 ? m1 : m2);
			args.add(m1 < m2 ? m2 : m1);
			query.executeWithArray(args.toArray());
			query.closeAll();
		}
	}
	
	public void updateUserAvg(long m1) 
	{
		try {
			Util.getUserPeer(pm).updateUserStat(m1);
		} catch (APIException e) {
		}
	}

	@Override
	public Double getAvgContentRating(long c, int type) {
		try {
			return Util.getItemPeer(pm).getItemAvgRating(c, type);
		} catch (APIException e) {
			return Constants.DEFAULT_OPINION_VALUE;
		}
	}
	
	public void updateContentAvg(long content)
	{
		try {
			Util.getItemPeer(pm).updateItemStat(content);
		} catch (APIException e) {
		}
	}

	@Override
	public int getNumberSharedOpinions(long u1, long u2, int type) {
		// TODO Auto-generated method stub
		return 0;
	}

	public Collection<Long> sort(Long userId,List<Long> items) {
		String filter = "";
		for(Long i : items) { filter += "'"+ i + "',"; }
		filter = filter.substring(0,filter.length()-1);
		String q = "select p.item_id from (select item_id,client_item_id from items where item_id in ("+filter+")) p left join item_map_enum m on p.item_id=m.item_id left join dimension d on m.attr_id=d.attr_id and m.value_id=d.value_id left join user_dim ud on ud.user_id=? and d.dim_id=ud.dim_id left join network n on n.u1=?  left join actions o on o.item_id=p.item_id and n.u2=o.user_id group by p.item_id order by coalesce(sum(o.value),1)*coalesce(1 + sum(relevance),1) desc,p.item_id desc";
		Query query = pm.newQuery("javax.jdo.query.SQL",q);
		return (Collection<Long>) query.execute(userId, userId);
	}
}
