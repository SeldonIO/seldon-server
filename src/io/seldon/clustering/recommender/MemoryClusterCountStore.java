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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import io.seldon.api.TestingUtils;

/**
 * Need map of clusters (fixed size) -> itemMaps
 * itemMap concurrentMap of Long (itemId) -> WeightedScore (with simple synchronize method to update)
 * @author rummble
 *
 */
public class MemoryClusterCountStore implements ClusterCountStore {

	private static Logger logger = Logger.getLogger(MemoryClusterCountStore.class.getName());
	
	public enum COUNTER_TYPE {SIMPLE,DECAY};
	private COUNTER_TYPE counterType;
	private ConcurrentMap<Integer,ClusterCounter> map;
	private long lastUpdateTime = 0; // associates the counts with the last cluster update
	private int cacheSize = 100; // cache size for individual cluster counts
	private double threshold = 0; // counts below this are ignored
	private double alpha = 1; // exponential decay parameter
	
	public MemoryClusterCountStore(COUNTER_TYPE counterType,int cacheSize,double threshold,double alpha)
	{
		this.cacheSize = cacheSize;
		this.threshold = threshold;
		this.alpha = alpha;
		this.map = new ConcurrentHashMap<Integer,ClusterCounter>();
		this.counterType = counterType;
	}
	
	@Override
	public Map<Long, Double> getTopCounts(long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		Map<Long,Double> counts = new HashMap<Long,Double>();
		for(Integer c : map.keySet())
		{
			Map<Long,Double> countsCluster = map.get(c).getTopCounts(time, limit);
			for(Map.Entry<Long,Double> e : countsCluster.entrySet())
			{
				Double current = counts.get(e.getKey());
				if (current == null)
					current = 0D;
				counts.put(e.getKey(), current + e.getValue());
			}
		}
		return counts;
	}
	
	@Override
	public Map<Long, Double> getTopCountsByDimension(int dimension,
		 long time, int limit, double decay)
			throws ClusterCountNoImplementationException {
		throw new ClusterCountNoImplementationException();
	}
	
	@Override
	public Map<Long, Double> getTopCounts(int clusterId, long timestamp,
			long time, int limit, double decay) {
		ClusterCounter c = map.get(clusterId);
		if (c != null)
			return c.getTopCounts(time, limit);
		else
			return new HashMap<Long,Double>();
	}
	
	@Override
	public Map<Long, Double> getTopCountsByDimension(int clusterId, int dimension,
			long timestamp, long time, int limit, double decay)
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
	/**
	 * Reset map and set new timestamp
	 * @param timestamp
	 */
	private synchronized void reset(long timestamp)
	{
		if (timestamp > this.lastUpdateTime)
		{
			this.map = new ConcurrentHashMap<Integer,ClusterCounter>();
			this.lastUpdateTime = timestamp;
		}
	}
	
	private ClusterCounter createClusterCounter()
	{
		switch(counterType)
		{
		case SIMPLE:
			return new MemoryCachingClusterCountMap(cacheSize);
		case DECAY:
			return new MemoryWeightedClusterCountMap(cacheSize,alpha);
		default:
			return new MemoryCachingClusterCountMap(cacheSize);
		}
	}
	
	
	
	@Override
	public void add(int clusterId, long itemId,double weight,long timestamp)
	{
		add(clusterId,itemId,weight,timestamp,TestingUtils.getTime());
	}
	
	@Override
	public void add(int clusterId, long itemId,double weight,long timestamp,long time) {
		
		if (this.lastUpdateTime == 0)
			reset(timestamp);

		if (timestamp == this.lastUpdateTime)
		{
			ClusterCounter cmap = map.get(clusterId); // most of the time this will not be null
			if (cmap != null)
			{
				cmap.incrementCount(itemId, weight,time);
			}
			else
			{
				map.putIfAbsent(clusterId, createClusterCounter());
				add(clusterId,itemId,weight,timestamp,time);
			}
		}
		else
		{
			if (timestamp > this.lastUpdateTime)
			{
				reset(timestamp);
				add(clusterId,itemId,weight,timestamp,time);
			}
			else
			{
				//ignore old cluster 
			}
		}
	}
	
	private double doThreshold(double count)
	{
		if (count < threshold)
			return 0;
		else
			return count;
	}
	
	@Override
	public double getCount(int clusterId, long itemId,long timestamp,long time) {
		if (timestamp == 0)
			this.lastUpdateTime = timestamp;
		if (timestamp == this.lastUpdateTime)
		{
			ClusterCounter cmap = map.get(clusterId); // most of the time this will not be null
			if (cmap != null)
			{
				return doThreshold(cmap.getCount(itemId,time));
			}
			else
			{
				map.putIfAbsent(clusterId, createClusterCounter());
				return 0D;
			}
		}
		else
		{
			if (timestamp > this.lastUpdateTime)
				reset(timestamp);
			return 0;
		}
	}

	

	@Override
	public void setAlpha(double alpha) {
		// TODO Auto-generated method stub
		
	}


	public int getCacheSize() {
		return cacheSize;
	}


	public void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}


	public double getThreshold() {
		return threshold;
	}


	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}


	public double getAlpha() {
		return alpha;
	}


	@Override
	public boolean needsExternalCaching() {
		return false;
	}

	

	


	


	
}
