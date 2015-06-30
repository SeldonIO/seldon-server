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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;

/**
 * Small memory footprint user cluster store.
 * <ul>
 * <li> Stores clusters as 2 bytes, so cluster ids must be between 0 and 65536.
 * <li> Stores weight as a byte and assumes weight is between 0 and 1, so stores it as 1 of 256 ranges between 0 and 1
 * </ul>
 * @author rummble
 *
 */
public class MemoryUserClusterStore implements UserClusterStore {
	private static Logger logger = Logger.getLogger( MemoryUserClusterStore.class.getName() );
	
	private static final int CLUSTER_NUM_BYTES = 3;
	private static final int SHORT_RANGE = 32767;
	private static final double WEIGHT_INCR = 1f/255f;
	
	FastByIDMap<byte[]> store; // use Mahout memory efficient map (28 bytes per entry)
	String client;
	long timestamp = 0;
	Map<Integer,Integer> clusterGroups;
	boolean loaded = false;
	
	

	ConcurrentHashMap<Long,byte[]> transientClusters;
	
	public MemoryUserClusterStore(String client,int entries)
	{
		logger.info("MemoryUserClusterStore for "+client+" of size "+entries);
		this.store = new FastByIDMap<>(entries);
		this.client = client;
		this.clusterGroups = new ConcurrentHashMap<>();
		this.transientClusters = new ConcurrentHashMap<>();
	}
	
	/**
	 * Store clusters for user. This method is not thread safe.
	 * @param userId
	 * @param clusters
	 */
	public void store(long userId,List<UserCluster> clusters)
	{
		byte[] vals = new byte[clusters.size()*CLUSTER_NUM_BYTES];
		int count = 0;
		for(UserCluster cluster : clusters)
		{
			if (timestamp == 0)
				timestamp = cluster.timeStamp;
			int clusterId = cluster.getCluster();
			double weight = cluster.getWeight();
			if (clusterId > 65536)
			{
                final String message = "ClusterId is too big: " + clusterId + " for user " + userId+" for client "+client;
                final ClusterRecommenderException recommenderException = new ClusterRecommenderException(message);
                logger.error(message, recommenderException);
				throw recommenderException;
			}
			if (weight < 0 || weight > 1)
			{
                final String message = "Bad weight: " + weight + " for user " + userId;
                final ClusterRecommenderException recommenderException = new ClusterRecommenderException(message);
                logger.error(message, recommenderException);
                throw recommenderException;
			}
			clusterId = clusterId - SHORT_RANGE;
			byte cId1 = (byte)(clusterId & 0xff);
			byte cId2 = (byte) ((clusterId >> 8) & 0xff);
			byte w = (byte) (Math.round((weight/WEIGHT_INCR)) - 128);
			int index = count * 3;
			vals[index] = cId1;
			vals[index+1] = cId2;
			vals[index+2] = w;
			if (!clusterGroups.containsKey(clusterId))
				clusterGroups.put(clusterId, cluster.getGroup());
			count++;
		}
		if(loaded)
			this.transientClusters.put(userId, vals);
		else
			store.put(userId, vals);
	}

	@Override
	public List<UserCluster> getClusters(long userId) {
		List<UserCluster> clusters = new ArrayList<>();
		byte[] b = store.get(userId);
		if (b == null)
			b = this.transientClusters.get(userId);
		if (b != null)
		{
			for(int i=0;i<b.length;i=i+CLUSTER_NUM_BYTES)
			{
				int clusterId = ((short)(((b[i+1] & 0xff) << 8) | (b[i] & 0xff))) + SHORT_RANGE;
				double weight = (b[i+2]+128)*WEIGHT_INCR;
				if (logger.isDebugEnabled())
					logger.debug("ClusterId:"+clusterId+" for user "+userId);
				int group = 0;
				if (clusterGroups.containsKey(clusterId))
					group = clusterGroups.get(clusterId);
				UserCluster cluster = new UserCluster(userId,clusterId,weight,timestamp,group);
				clusters.add(cluster);
			}
		}
		return clusters;
	}
	
	public static void main(String[] args)
	{
		MemoryUserClusterStore m = new MemoryUserClusterStore("",1);
		ArrayList<UserCluster> clusters = new ArrayList<>();
		clusters.add(new UserCluster(1L,2,0.75,1,1));
		clusters.add(new UserCluster(1L,14561,0.75,1,1));
		clusters.add(new UserCluster(1L,62421,0.75,1,1));
		System.out.println("Clusters created:");
		for(UserCluster cluster : clusters)
			System.out.println(cluster.toString());
		m.store(1L, clusters);
		List<UserCluster> clustersGot = m.getClusters(1L);
		System.out.println("Clusters returned:");
		for(UserCluster cluster : clustersGot)
			System.out.println(cluster.toString());
	}

	@Override
	public List<UserCluster> getClusters() {
		return null;
	}

	@Override
	public int getNumUsersWithClusters() {
		return store.size();
	}

	@Override
	public long getCurrentTimestamp() {
	return timestamp;
	}

	@Override
	public boolean needsExternalCaching() {
		return false;
	}
	
	
	public boolean isLoaded() {
		return loaded;
	}

	public void setLoaded(boolean loaded) {
		this.loaded = loaded;
	}
}
