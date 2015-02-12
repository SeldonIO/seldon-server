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
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.recommender.UserCluster;
import org.apache.log4j.Logger;

import io.seldon.clustering.recommender.TransientUserClusterStore;
import io.seldon.clustering.recommender.UserClusterStore;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;

public class JdoUserClusterStore implements UserClusterStore, TransientUserClusterStore {

	private static Logger logger = Logger.getLogger(JdoUserClusterStore.class.getName());
	
	PersistenceManager pm;
	
	public JdoUserClusterStore(PersistenceManager pm)
	{
		this.pm = pm;
	}
	
	public void addTransientCluster(final List<UserCluster> clusters)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	for(UserCluster cluster : clusters)
			    	{
			    		Query query = pm.newQuery( "javax.jdo.query.SQL","insert into user_clusters_transient (t_id,user_id,cluster_id,weight) values (0,?,?,?)");
			    		query.execute(cluster.getUser(), cluster.getCluster(), cluster.getWeight());
			    		query.closeAll();
			    	}
			    }});
		} catch (DatabaseException e) 
		{
			if (clusters.size() > 0)
				logger.error("Failed to Add Transient cluster for user "+clusters.get(0).getUser(), e);
			else
				logger.error("Failed to add empty transient clusters", e);
		}
		
	}
	
	
	public TransientUserClusters getTransientClusters(long checkpoint) {
		
		Query query = pm.newQuery( "javax.jdo.query.SQL","select max(t_id) from user_clusters_transient");
		query.setResultClass(Long.class);
		query.setUnique(true);
		Long lastId =  (Long) query.execute();
		if (lastId != null && lastId > checkpoint)
		{
			logger.debug("Loading new transient clusters as checkpoint is "+checkpoint+" and found checkpoint is " + lastId);
			query = pm.newQuery( "javax.jdo.query.SQL","select user_id,t.cluster_id,weight,lastupdate,group_id from user_clusters_transient t, cluster_update, cluster_group where t.cluster_id=cluster_group.cluster_id and t.t_id>? order by user_id asc");
			query.setResultClass(UserCluster.class);
			List<UserCluster> clusters =  (List<UserCluster>) query.execute(checkpoint);
			return new TransientUserClusters(lastId,new ArrayList<UserCluster>(clusters));
		}
		else
			return new TransientUserClusters(lastId,new ArrayList<UserCluster>());
	}
	
	@Override
	public List<UserCluster> getClusters(long userId) {
		Query query = pm.newQuery( "javax.jdo.query.SQL","select user_id,user_clusters.cluster_id,weight,lastupdate,group_id from user_clusters, cluster_update, cluster_group where user_id=? and user_clusters.cluster_id=cluster_group.cluster_id");
		query.setResultClass(UserCluster.class);
		return (List<UserCluster>) query.execute(userId);
	}


	@Override
	public List<UserCluster> getClusters() {
		Query query = pm.newQuery( "javax.jdo.query.SQL","select user_id,user_clusters.cluster_id,weight,lastupdate,group_id from user_clusters, cluster_update, cluster_group where user_clusters.cluster_id=cluster_group.cluster_id order by user_id asc");
		query.setResultClass(UserCluster.class);
		return (List<UserCluster>) query.execute();
	}


	@Override
	public int getNumUsersWithClusters() {
		Query query = pm.newQuery( "javax.jdo.query.SQL","select count(distinct user_id) from user_clusters");
		query.setResultClass(Integer.class);
		query.setUnique(true);
		return (Integer) query.execute();
	}


	@Override
	public long getCurrentTimestamp() {
		Query query = pm.newQuery( "javax.jdo.query.SQL","select lastupdate from cluster_update");
		query.setResultClass(Long.class);
		query.setUnique(true);
		return (Long) query.execute();
	}


	@Override
	public boolean needsExternalCaching() {
		return true;
	}

	public static class TransientUserClusters
	{
		Long checkpoint;
		List<UserCluster> clusters;
		public TransientUserClusters(Long checkpoint, List<UserCluster> clusters) {
			super();
			this.checkpoint = checkpoint;
			this.clusters = clusters;
		}
		public Long getCheckpoint() {
			return checkpoint;
		}
		public List<UserCluster> getClusters() {
			return clusters;
		}
		
		
		
	}

}
