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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.recommender.ClusterReferrer;
import io.seldon.clustering.recommender.IClusterFromReferrer;
import io.seldon.db.jdo.JDOFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class JdoClusterFromReferrer implements IClusterFromReferrer {

	private static Logger logger = Logger.getLogger( JdoClusterFromReferrer.class.getName() );
	
	public static final int RELOAD_SECS = 900;
	
	String client;
	ConcurrentHashMap<String,Integer> clusterMap = new ConcurrentHashMap<>();
	ReentrantReadWriteLock lock;
	Timer reloadTimer;
	public JdoClusterFromReferrer(String client)
	{
		this.client = client;
		lock = new ReentrantReadWriteLock(true);
		updateClusterMap();
		startReloadTransientTimer(RELOAD_SECS);
	}
	
	public JdoClusterFromReferrer(String client,int reloadSecs)
	{
		this.client = client;
		lock = new ReentrantReadWriteLock(true);
		updateClusterMap();
		startReloadTransientTimer(reloadSecs);
	}

	private void updateClusterMap()
	{
		PersistenceManager pm = JDOFactory.getPersistenceManager(client);
		if (pm != null)
		{
			Query query = pm.newQuery( "javax.jdo.query.SQL","select referrer,cluster from cluster_referrer");
			query.setResultClass(ClusterReferrer.class);
			List<ClusterReferrer> res = (List<ClusterReferrer>) query.execute();
			logger.info("Getting READ lock");
			lock.writeLock().lock();
			try
			{
				ConcurrentHashMap<String,Integer> clusterMapNew = new ConcurrentHashMap<>();
				for(ClusterReferrer r : res)
				{
					logger.info("Updating cluster map for "+client+" with referrer:"+r.getReferrer()+" cluster:"+r.getCluster());
					clusterMapNew.put(r.getReferrer(), r.getCluster());
				}

				clusterMap = clusterMapNew;
			}
			finally
			{
				lock.writeLock().unlock();
				logger.info("Released WRITE lock");
			}
			
		}
		else
			logger.error("Failed to get persistence manager for client "+client);
	}
	
	@Override
	public Set<Integer> getClusters(String referrer) {
		if (StringUtils.isNotEmpty(referrer))
		{
			lock.readLock().lock();
			try
			{
				Set<Integer> clusters = new HashSet<>();
				for(Map.Entry<String,Integer> e : clusterMap.entrySet())
				{
					if (referrer.startsWith(e.getKey()))
						clusters.add(e.getValue());
				}
				return clusters;
			}
			finally
			{
				lock.readLock().unlock();
			}
		}
		return null;
	}
	
	public void shutdown()
	{
		if (reloadTimer != null)
			reloadTimer.cancel();
	}
	
	private void startReloadTransientTimer(final int delaySeconds)
	{
		reloadTimer = new Timer(true);
		int period = 1000 * delaySeconds;
		int delay = 1000 * delaySeconds;

		
		reloadTimer.scheduleAtFixedRate(new TimerTask() {
			   public void run()  
			   {
				   try
				   {
					   logger.info("About to update cluster map for client "+client);
					   updateClusterMap();
					   logger.info("Updated cluster map for client "+client);
				   }
				   catch (Exception e)
				   {
					   logger.error("Caught exception trying to load transient clusters",e);
				   }
				   finally
				   {
					   JDOFactory.cleanupPM();
				   }
			   }
		   }, delay, period);
		
	}
	

}
