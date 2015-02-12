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

package io.seldon.clustering.recommender;

import java.util.Map;

import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import net.spy.memcached.CASMutation;

import org.apache.log4j.Logger;

public class MemcacheClusterCountStore implements ClusterCountStore {
	
	private static Logger logger = Logger.getLogger(MemcacheClusterCountStore.class.getName());
	
	String client;
	int timeout;
	double threshold = 0;
	
	public MemcacheClusterCountStore(String client,int timeout,double threshold) {
		super();
		this.client = client;
		this.timeout = timeout;
		this.threshold = threshold;
	}

	private void storeSimple(int clusterId,long itemId,long clusterTimestamp,final Double weight,final Long time)
	{
		logger.debug("Storing in cluster "+clusterId+" for item "+itemId+" clusterTimestamp: "+clusterTimestamp+" weight: "+weight);
		// This is how we modify a list when we find one in the cache.
	    CASMutation<Double> mutation = new CASMutation<Double>() {

	        // This is only invoked when a value actually exists.
	        public Double getNewValue(Double current) {
	             return current+weight;
	        }
	    };
	    
	    MemCachePeer.cas(MemCacheKeys.getClusterCountKey(client, clusterId, itemId, clusterTimestamp), mutation, weight, timeout);
	    
	}

	@Override
	public void setAlpha(double alpha) {
		
	}

	@Override
	public void add(int clusterId, long itemId, double weight,
			long clusterTimestamp) {
		this.storeSimple(clusterId, itemId, clusterTimestamp, weight,null);
		
	}

	@Override
	public void add(int clusterId, long itemId, double weight,
			long clusterTimestamp, long time) {
		this.storeSimple(clusterId, itemId, clusterTimestamp, weight,time);
	}

	@Override
	public double getCount(int clusterId, long itemId, long timestamp, long time) {
		Double count = (Double) MemCachePeer.get(MemCacheKeys.getClusterCountKey(client, clusterId, itemId, timestamp));
		if (count != null && count > threshold)
		{
			logger.debug("Returning count "+count+" for cluster "+clusterId+" for item "+itemId+" timestamp "+timestamp);
			return count;
		}
		else
		{
			logger.debug("Returning no count (0) for cluster "+clusterId+" for item "+itemId+" timestamp "+timestamp);
			return 0;
		}
	}

	@Override
	public boolean needsExternalCaching() {
		return true;
	}

	@Override
	public Map<Long, Double> getTopCounts(int clusterId, long timestamp,
			long time, int limit, double decay) throws ClusterCountNoImplementationException {
		logger.warn("getTopCounts not implemented but called!");
		throw new ClusterCountNoImplementationException();
	}

	@Override
	public Map<Long, Double> getTopCountsByDimension(int clusterId, int dimension,
			long timestamp, long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		throw new ClusterCountNoImplementationException();
	}
	
	@Override
	public Map<Long, Double> getTopCounts(long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		throw new ClusterCountNoImplementationException();
	}
	
	@Override
	public Map<Long, Double> getTopCountsByDimension(int dimension,
		 long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		throw new ClusterCountNoImplementationException();
	}

	@Override
	public Map<Long, Double> getTopCountsByTwoDimensions(int dimension1,
			int dimension2, long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		throw new ClusterCountNoImplementationException();
	}

	@Override
	public Map<Long, Double> getTopSignificantCountsByDimension(int clusterId,
			int dimension, long timestamp, long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		throw new ClusterCountNoImplementationException();
	}
}
