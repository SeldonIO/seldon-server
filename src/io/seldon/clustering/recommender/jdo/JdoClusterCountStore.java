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

package io.seldon.clustering.recommender.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.TestingUtils;
import io.seldon.clustering.recommender.ClusterCountStore;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import org.apache.log4j.Logger;

import io.seldon.clustering.recommender.ClusterCountNoImplementationException;

public class JdoClusterCountStore extends ClientPersistable implements ClusterCountStore {

	private static Logger logger = Logger.getLogger(JdoClusterCountStore.class.getName());

	double alpha = 3600;
	
	public JdoClusterCountStore(String client)
	{
		super(client);
	}
	
	
	
	@Override
	public void add(final int clusterId, final long itemId, final double weight,
			long clusterTimestamp) {
		
		AsyncClusterCountStore asyncStore = AsyncClusterCountFactory.get().get(this.clientName);
		if (asyncStore != null)
		{
			asyncStore.put(new AsyncClusterCountStore.ClusterCount(clusterId,itemId, TestingUtils.getTime(),weight));
		}
		else
		{
			
			final PersistenceManager pm = getPM();
			
			try {
				TransactionPeer.runTransaction(new Transaction(pm) {
					public void process() {

						Query query = pm.newQuery("javax.jdo.query.SQL", "insert into cluster_counts values (?,?,?,unix_timestamp()) on duplicate key update count=?+exp(-(unix_timestamp()-t)/?)*count,t=unix_timestamp();");
						ArrayList<Object> args = new ArrayList<Object>();
						args.add(clusterId);
						args.add(itemId);
						args.add(weight);
						args.add(weight);
						args.add(alpha);
						query.executeWithArray(args.toArray());


					}
				});
			} catch (DatabaseException e)
			{
				logger.error("Failed to Add count", e);
			}
		}
		
	}
	
	/**
	 * timestamp is ignore.
	 */
	@Override
	public void add(final int clusterId, final long itemId,final double weight,long timestamp,final long time) {
		
		AsyncClusterCountStore asyncStore = AsyncClusterCountFactory.get().get(this.clientName);
		if (asyncStore != null)
		{
			asyncStore.put(new AsyncClusterCountStore.ClusterCount(clusterId,itemId,time,weight));
		}
		else
		{
			final PersistenceManager pm = getPM();
			
			try {
				TransactionPeer.runTransaction(new Transaction(pm) { 
				    public void process()
				    { 
				    
				    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into cluster_counts values (?,?,?,unix_timestamp()) on duplicate key update count=?+exp(-(greatest(unix_timestamp()-t,0)/?))*count,t=unix_timestamp();");
				    	ArrayList<Object> args = new ArrayList<Object>();
				    	args.add(clusterId);
				    	args.add(itemId);
				    	args.add(weight);
				    	args.add(weight);
				    	args.add(alpha);
				    	query.executeWithArray(args.toArray());
				    
				    	
				    }});
			} catch (DatabaseException e) 
			{
				logger.error("Failed to Add count", e);
			}
		}
	}
	

	/**
	 * timestamp and time is ignore for db counts - the db value for these is used. They are assumed to be up-todate with clusters.
	 */
	@Override
	public double getCount(int clusterId, long itemId,long timestamp,long time) {
		final PersistenceManager pm = getPM();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select count from cluster_counts where id=? and item_id=?" );
		query.setResultClass(Double.class);
		query.setUnique(true);
		Double count = (Double) query.execute(clusterId, itemId);
		if (count != null)
			return count;
		else
			return 0D;
	}

	

	@Override
	public void setAlpha(double alpha) {
		this.alpha = alpha;
	}

	@Override
	public boolean needsExternalCaching() {
		return true;
	}



	/**
	 * timestamp and time is ignore for db counts - the db value for these is used. They are assumed to be up-to-date with clusters.
	 */
	@Override
	public Map<Long, Double> getTopCounts(int clusterId, long timestamp,
			long time, int limit, double decay) {
		final PersistenceManager pm = getPM();
		Map<Long,Double> map = new HashMap<Long,Double>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_id,exp(-(greatest(unix_timestamp()-t,0)/?))*count as decayedCount from cluster_counts where id=? order by decayedCount desc limit "+limit );
		Collection<Object[]> res = (Collection<Object[]>)  query.execute(decay,clusterId);
		for(Object[] r : res)
		{
			Long itemId = (Long) r[0];
			Double count = (Double) r[1];
			map.put(itemId, count);
		}
		return map;
	}

	//TODO - need to use decay/alpha
	//ignore time use db time
	@Override
	public Map<Long, Double> getTopCounts(long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		final PersistenceManager pm = getPM();
		Map<Long,Double> map = new HashMap<Long,Double>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_id,sum(exp(-(greatest(unix_timestamp()-t,0)/?))*count) as decayedSumCount from cluster_counts group by item_id order by decayedSumCount desc limit "+limit );
		Collection<Object[]> res = (Collection<Object[]>)  query.execute(decay);
		for(Object[] r : res)
		{
			Long itemId = (Long) r[0];
			Double count = (Double) r[1];
			map.put(itemId, count);
		}
		return map;
	}


	@Override
	public Map<Long, Double> getTopCountsByDimension(int clusterId, int dimension,
			long timestamp, long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		final PersistenceManager pm = getPM();
		Map<Long,Double> map = new HashMap<Long,Double>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_id,exp(-(greatest(unix_timestamp()-t,0)/?))*count as decayedCount from cluster_counts natural join item_map_enum natural join dimension where id = ? and dim_id = "+dimension+" order by decayedCount desc limit "+limit );
		Collection<Object[]> res = (Collection<Object[]>)  query.execute(decay,clusterId);
		for(Object[] r : res)
		{
			Long itemId = (Long) r[0];
			Double count = (Double) r[1];
			map.put(itemId, count);
		}
		return map;
	}

	@Override
	public Map<Long, Double> getTopSignificantCountsByDimension(int clusterId,
			int dimension, long timestamp, long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		final PersistenceManager pm = getPM();
		Map<Long,Double> map = new HashMap<Long,Double>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_id,r.v*r.count as score from (select item_id,(count/sl-s/sg)/greatest(count/sl,s/sg) as v,count from (select exp(-(greatest(unix_timestamp()-c.t,0)/?))*c.count as count,cit.total as s,sl,cct.total as sg,c.item_id from cluster_counts c join (select sum(exp(-(greatest(unix_timestamp()-c.t,0)/?))) sl from cluster_counts c where id=?) t1 join cluster_counts_total cct join cluster_counts_item_total cit on (c.item_id=cit.item_id) where id=?) r1) r natural join item_map_enum natural join dimension where dim_id = ? order by score desc limit "+limit );
		ArrayList<Object> args = new ArrayList<Object>();
		args.add(decay);
		args.add(decay);
		args.add(clusterId);
		args.add(clusterId);
		args.add(dimension);
		Collection<Object[]> res = (Collection<Object[]>)  query.executeWithArray(args.toArray());
		for(Object[] r : res)
		{
			Long itemId = (Long) r[0];
			Double count = (Double) r[1];
			map.put(itemId, count);
		}
		return map;
	}


	@Override
	public Map<Long, Double> getTopCountsByDimension(int dimension,
		long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		final PersistenceManager pm = getPM();
		Map<Long,Double> map = new HashMap<Long,Double>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_id,sum(exp(-(greatest(unix_timestamp()-t,0)/?))*count) as decayedSumCount from cluster_counts natural join item_map_enum natural join dimension where dim_id = ? group by item_id order by decayedSumCount desc limit "+limit );
		Collection<Object[]> res = (Collection<Object[]>)  query.execute(decay,dimension);
		for(Object[] r : res)
		{
			Long itemId = (Long) r[0];
			Double count = (Double) r[1];
			map.put(itemId, count);
		}
		return map;
	}


	@Override
	public Map<Long, Double> getTopCountsByTwoDimensions(int dimension1,
			int dimension2, long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		final PersistenceManager pm = getPM();
		Map<Long,Double> map = new HashMap<Long,Double>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select c.item_id,sum(exp(-(greatest(unix_timestamp()-t,0)/?))*count) as decayedCount from cluster_counts c natural join item_map_enum ime1 join dimension d1 on (d1.attr_id=ime1.attr_id and ime1.value_id=d1.value_id) join item_map_enum ime2 on (c.item_id=ime2.item_id) join dimension d2 on (d2.attr_id=ime2.attr_id and ime2.value_id=d2.value_id) where d1.dim_id = ? and d2.dim_id = ?  group by item_id order by decayedcount desc limit "+limit );
		ArrayList<Object> args = new ArrayList<Object>();
		args.add(decay);
		args.add(dimension1);
		args.add(dimension2);
		Collection<Object[]> res = (Collection<Object[]>)  query.executeWithArray(args.toArray());
		for(Object[] r : res)
		{
			Long itemId = (Long) r[0];
			Double count = (Double) r[1];
			map.put(itemId, count);
		}
		return map;
	}



	

	



	



}
