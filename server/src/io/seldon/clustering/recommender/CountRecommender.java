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

import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.general.ItemPeer;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.UpdateRetriever;
import io.seldon.trust.impl.jdo.RecommendationUtils;
import io.seldon.util.CollectionTools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

public class CountRecommender {

	private static Logger logger = Logger.getLogger(CountRecommender.class.getName());
	
	UserClusterStore userClusters;
	ClusterCountStore clusterCounts;
	String client;
	boolean fillInZerosWithMostPopular = true;
	boolean testMode = false;
	String referrer;
	
	private static int EXPIRE_COUNTS = 300;
	private static int EXPIRE_USER_CLUSTERS = 600;
	public static final int BUCKET_CLUSTER_ID = -1;
	
	
	public static void initialise(Properties props)
	{
		String expireCountsTimeout = props.getProperty("io.seldon.memcache.countrec.count.timeout");
		if (expireCountsTimeout != null)
		{
			EXPIRE_COUNTS = Integer.parseInt(expireCountsTimeout);
		}
		String expireClustersTimeout = props.getProperty("io.seldon.memcache.countrec.clusters.timeout");
		if (expireClustersTimeout != null)
		{
			EXPIRE_USER_CLUSTERS = Integer.parseInt(expireClustersTimeout);
		}
		logger.info("Expire timeout for count recommender counts:"+EXPIRE_COUNTS);
		logger.info("Expire timeout for count recommender clusters:"+EXPIRE_USER_CLUSTERS);
	}
	
	public CountRecommender(String client,UserClusterStore userClusters,
			ClusterCountStore clusterCounts) {
		this.userClusters = userClusters;
		this.clusterCounts = clusterCounts;
		this.client = client;
		this.testMode = TestingUtils.get().getTesting();
		logger.debug("Test mode is "+testMode);
	}
	
	
	
	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	
	
	private Set<Integer> getReferrerClusters()
	{
		if (referrer != null)
		{
			ClusterFromReferrerPeer p = ClusterFromReferrerPeer.get();
			if (p != null)
				return p.getClustersFromReferrer(client, referrer);
			else
				return null;
		}
		else
			return null;
	}

	
	/**
	 * 
	 * @param userId
	 * @param itemId
	 * @param time - in secs
	 */
	public void addCount(long userId,long itemId,long time, boolean useBucketCluster,Double actionWeight)
	{
		if (actionWeight == null) actionWeight = 1.0D;
		List<UserCluster> clusters = getClusters(userId,null);
		if (clusters != null && clusters.size()>0)
		{
			for(UserCluster cluster : clusters)
				clusterCounts.add(cluster.getCluster(), itemId,cluster.getWeight() * actionWeight,cluster.getTimeStamp(),time);
		} else if(useBucketCluster) {
            clusterCounts.add(BUCKET_CLUSTER_ID, itemId, actionWeight, 0,time);
        }

		Set<Integer> referrerClusters = getReferrerClusters();
		if (referrerClusters != null)
		{
			for (Integer cluster : referrerClusters)
			{
				clusterCounts.add(cluster, itemId,actionWeight,0,time);
			}
		}
	}
	
	public void addCount(long userId,long itemId, boolean useBucketCluster,Double actionWeight)
	{
		if (actionWeight == null) actionWeight = 1.0D;
		List<UserCluster> clusters = getClusters(userId,null);
		if (clusters != null && clusters.size() > 0)
		{
			for(UserCluster cluster : clusters)
				clusterCounts.add(cluster.getCluster(), itemId,cluster.getWeight()*actionWeight,cluster.getTimeStamp());
		} else if(useBucketCluster){
            clusterCounts.add(BUCKET_CLUSTER_ID, itemId, actionWeight, 0);
        }
		Set<Integer> referrerClusters = getReferrerClusters();
		if (referrerClusters != null)
		{
			for (Integer cluster : referrerClusters)
			{
				clusterCounts.add(cluster, itemId, actionWeight, 0);
			}
		}
	}
	
	public Map<Long,Double> recommendUsingItem(String recommenderType, long itemId,int dimension,int numRecommendations,Set<Long> exclusions,double decay,String clusterAlg,int minNumItems)
	{
		boolean checkDimension = !(dimension == Constants.DEFAULT_DIMENSION || dimension == Constants.NO_TRUST_DIMENSION);
		int minAllowed = minNumItems < numRecommendations ? minNumItems : numRecommendations;
		logger.debug("Recommend using items - dimension "+dimension+" num recomendations "+numRecommendations+" itemId "+itemId+" minAllowed:"+minAllowed+" client "+client);
		Map<Long,Double> res = new HashMap<>();
		if (clusterAlg != null)
		{
			//get the cluster id from the item if possibl
			List<UserCluster> clusters = new ArrayList<>();
			switch(clusterAlg)
			{
			case "NONE":
			case "LDA_USER":
				break;
			case "DIMENSION":
				//get dimension for item
				logger.debug("Getting cluster for item "+itemId+" from dimension");
				Collection<Integer> dims = ItemService.getItemDimensions(new ConsumerBean(client), itemId);
				if (dims != null)
					for(Integer d : dims)
						clusters.add(new UserCluster(0, d, 1.0D, 0, 0));
				break;
			case "LDA_ITEM":
				//get cluster from item_clusters table
				logger.debug("Getting cluster for item "+itemId+" from item cluster");
				Integer dim = ItemService.getItemCluster(new ConsumerBean(client), itemId);
				if (dim != null)
					clusters.add(new UserCluster(0, dim, 1.0D, 0, 0));
				break;
			}
			
			if (clusters.size() > 0)
			{
				Map<Long,Double> counts = new HashMap<>();
				int numTopCounts = numRecommendations * 2; // number of counts to get - defaults to twice the final number recommendations to return
				for(UserCluster cluster : clusters)
				{
					updateCounts(recommenderType,0, cluster, dimension, checkDimension, numTopCounts, exclusions, counts, 1.0D,decay);
				}
			

				if (counts.keySet().size() < minAllowed)
				{
					logger.debug("Number of items found "+counts.keySet().size()+" is less than "+minAllowed+" so returning empty recommendation for cluster item recommender for item "+itemId);
					return new HashMap<>();
				}
				
				res = RecommendationUtils.rescaleScoresToOne(counts, numRecommendations);
			}
			else
				logger.debug("No clusters for item "+itemId+" so returning empty results for item cluster count recommender");
		}
		return res;
	}
	

		
	private void updateCounts(String recommenderType,long userId,UserCluster cluster,int dimension,boolean checkDimension,int numTopCounts,Set<Long> exclusions,Map<Long,Double> counts,double clusterWeight,double decay)
	{
		Map<Long,Double> itemCounts = null;
		boolean localDimensionCheckNeeded = false;
		if (checkDimension)
		{
			try 
			{
				itemCounts = getClusterTopCountsForDimension(recommenderType, cluster.getCluster(), dimension,cluster.getTimeStamp(), numTopCounts,decay);
			} 
			catch (ClusterCountNoImplementationException e) 
			{
				localDimensionCheckNeeded = true;
			}
		}
		
		if (itemCounts == null)
		{
			try 
			{
				itemCounts = getClusterTopCounts(cluster.getCluster(),cluster.getTimeStamp(), numTopCounts,decay);
			}
			catch (ClusterCountNoImplementationException e) 
			{
				logger.error("Failed to get cluster counts as method not implemented",e);
				itemCounts = new HashMap<>();
			}
		}
		double maxCount = 0;
		for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
			if (itemCount.getValue() > maxCount)
				maxCount = itemCount.getValue();
		if (maxCount > 0)
		{
			ItemPeer iPeer = null;
			if (localDimensionCheckNeeded)
				iPeer = Util.getItemPeer(client);
			for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
			{
				Long item = itemCount.getKey();
				if (checkDimension && localDimensionCheckNeeded && !ItemService.getItemDimensions(new ConsumerBean(client),item).contains(dimension))
					continue;
				if (!exclusions.contains(item))
				{
					Double count = (itemCount.getValue()/maxCount) * cluster.getWeight() * clusterWeight; // weight cluster count by user weight for cluster and long term weight
					logger.debug("Adding long term count "+count+" for item "+item+" for user "+userId);
					Double existing = counts.get(item);
					if (existing != null)
						count = count + existing;
					counts.put(item, count);
				}
				else
					logger.debug("Ignoring excluded long term item in cluster recommendation "+item+" for user "+userId);
			}
		}
	}
	
	public Map<Long,Double> recommendGlobal(int dimension,int numRecommendations,Set<Long> exclusions,double decay,Integer dimension2)
	{
		boolean checkDimension = !(dimension == Constants.DEFAULT_DIMENSION || dimension == Constants.NO_TRUST_DIMENSION);
		int numTopCounts = numRecommendations * 5;
		Map<Long,Double> itemCounts = null;
		boolean localDimensionCheckNeeded = false;
		if (checkDimension)
		{
			try 
			{
				itemCounts = getClusterTopCountsForDimension(dimension,numTopCounts,decay,dimension2);
			} 
			catch (ClusterCountNoImplementationException e) 
			{
				localDimensionCheckNeeded = true;
			}
		}
		
		if (itemCounts == null)
		{
			try 
			{
				itemCounts = getClusterTopCounts(numTopCounts,decay);
				if (itemCounts == null)
				{
					logger.warn("Got null itemcounts");
					itemCounts = new HashMap<>();
				}
			}
			catch (ClusterCountNoImplementationException e) 
			{
				logger.error("Failed to get cluster counts as method not implemented",e);
				itemCounts = new HashMap<>();
			}
		}
		int excluded = 0;
		for(Iterator<Map.Entry<Long, Double>> i = itemCounts.entrySet().iterator();i.hasNext();)
		{
			Map.Entry<Long, Double> e = i.next();
			Long item = e.getKey();
			//logger.debug("Item:"+item+" count:"+e.getValue());
			if (checkDimension && localDimensionCheckNeeded && !ItemService.getItemDimensions(new ConsumerBean(client),item).contains(dimension))
			{
				i.remove();
				excluded++;
			}
			else if (exclusions.contains(item))
			{
				i.remove();
				excluded++;
			}
		}
		logger.debug("Recommend global for dimension "+dimension+" numRecs "+numRecommendations+" decay "+decay+" #in map "+itemCounts.size()+" excluded "+excluded+ " client "+client);
		return RecommendationUtils.rescaleScoresToOne(itemCounts, numRecommendations);
	}
	
	/**
	 * Provide a set of recommendations for a user using the counts from their clusters
	 * @param userId
	 * @param group
	 * @param numRecommendations
	 * @param includeShortTermClusters
	 * @param longTermWeight
	 * @param shortTermWeight
	 * @return
	 */
	public Map<Long,Double> recommend(String recommenderType,long userId,Integer group,int dimension,int numRecommendations,Set<Long> exclusions,boolean includeShortTermClusters,double longTermWeight,double shortTermWeight,double decay,int minNumItems)
	{
		
		boolean checkDimension = !(dimension == Constants.DEFAULT_DIMENSION || dimension == Constants.NO_TRUST_DIMENSION);
		int minAllowed = minNumItems < numRecommendations ? minNumItems : numRecommendations;
		logger.debug("Recommend for user clusters - dimension "+dimension+" num recomendations "+numRecommendations+"minAllowed:"+minAllowed+ " client "+client+" user "+userId);

		// get user clusters pruned by group
		List<UserCluster> clusters;
		List<UserCluster> shortTermClusters;
		if (userId == Constants.ANONYMOUS_USER)
		{
			clusters = new ArrayList<>();
			shortTermClusters = new ArrayList<>();
		}	
		else
		{
			clusters = getClusters(userId,group);
			if (includeShortTermClusters)
				shortTermClusters = getShortTermClusters(userId, group);
			else
				shortTermClusters = new ArrayList<>();
		}
		// fail early
		Set<Integer> referrerClusters = getReferrerClusters();
		if (referrerClusters == null || referrerClusters.size() == 0)
		{
			if (!includeShortTermClusters && clusters.size() == 0)
			{
				logger.debug("User has no long term clusters and we are not including short term clusters - so returning empty recommendations");
				return new HashMap<>();
			}
			else if (includeShortTermClusters && clusters.size() == 0 && shortTermClusters.size() == 0)
			{
				logger.debug("User has no long or short term clusters - so returning empty recommendations");
				return new HashMap<>();
			}
		}
		List<Long> res = null;
		Map<Long,Double> counts = new HashMap<>();
		int numTopCounts = numRecommendations * 5; // number of counts to get - defaults to twice the final number recommendations to return
		logger.debug("recommending using long term cluster weight of "+longTermWeight+" and short term cluster weight "+shortTermWeight+" decay "+decay);
		for(UserCluster cluster : clusters)
		{
			updateCounts(recommenderType,userId, cluster, dimension, checkDimension, numTopCounts, exclusions, counts, longTermWeight,decay);
		}
		for(UserCluster cluster : shortTermClusters)
		{
			updateCounts(recommenderType, userId, cluster, dimension, checkDimension, numTopCounts, exclusions, counts, shortTermWeight,decay);
		}

		if (referrerClusters != null)
		{
			logger.debug("Adding "+referrerClusters.size()+" referrer clusters to counts for user "+userId+" client "+client);
			for(Integer c : referrerClusters)
			{
				UserCluster uc = new UserCluster(userId, c, 1.0, 0, 0);
				updateCounts(recommenderType,userId, uc, dimension, checkDimension, numTopCounts, exclusions, counts, longTermWeight,decay);
			}
		}
		
		if (counts.keySet().size() < minAllowed)
		{
			logger.debug("Number of items found "+counts.keySet().size()+" is less than "+minAllowed+" so returning empty recommendation for user "+userId+" client "+client);
			return new HashMap<>();
		}
		
		return RecommendationUtils.rescaleScoresToOne(counts, numRecommendations);
	}	
	
	public List<Long> sort(long userId,List<Long> items,Integer group)
	{
		return this.sort(userId, items, group, false, 1.0D, 1.0D);
	}
	
	
	public List<Long> sort(long userId,List<Long> items,Integer group,boolean includeShortTermClusters,double longTermWeight,double shortTermWeight)
	{
		// get user clusters pruned by group
		List<UserCluster> clusters = getClusters(userId,group);
		List<UserCluster> shortTermClusters;
		if (includeShortTermClusters)
			shortTermClusters = getShortTermClusters(userId, group);
		else
			shortTermClusters = new ArrayList<>();
		if (!includeShortTermClusters && clusters.size() == 0)
		{
			logger.debug("User has no long term clusters and we are not including short term clusters - so returning empty recommendations");
			return new ArrayList<>();
		}
		else if (includeShortTermClusters && clusters.size() == 0 && shortTermClusters.size() == 0)
		{
			logger.debug("User has no long or short term clusters - so returning empty recommendations");
			return new ArrayList<>();
		}
		List<Long> res = null;
		Map<Long,Double> counts = new HashMap<>();
		for(long item : items) // initialise counts to zero
			counts.put(item, 0D);
		logger.debug("using long term cluster weight of "+longTermWeight+" and short term cluster weight "+shortTermWeight);
		for(UserCluster cluster : clusters)
		{
			Map<Long,Double> itemCounts = getClusterCounts(cluster.getCluster(),cluster.getTimeStamp(),items);
			double maxCount = 0;
			for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
				if (itemCount.getValue() > maxCount)
					maxCount = itemCount.getValue();
			if (maxCount > 0)
				for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
				{
					Long item = itemCount.getKey();
					Double count = (itemCount.getValue()/maxCount) * cluster.getWeight() * longTermWeight; // weight cluster count by user weight for cluster and long term weight
					logger.debug("Adding long term count "+count+" for item "+item+" for user "+userId);
					counts.put(item, counts.get(item) + count);
				}
		}
		for(UserCluster cluster : shortTermClusters)
		{
			Map<Long,Double> itemCounts = getClusterCounts(cluster.getCluster(),cluster.getTimeStamp(),items);
			double maxCount = 0;
			for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
				if (itemCount.getValue() > maxCount)
					maxCount = itemCount.getValue();
			if (maxCount > 0)
				for(Map.Entry<Long, Double> itemCount : itemCounts.entrySet())
				{
					Long item = itemCount.getKey();
					Double count = (itemCount.getValue()/maxCount) * cluster.getWeight() * shortTermWeight; // weight cluster count by user weight for cluster and short term weight
					logger.debug("Adding short term count "+count+" for item "+item+" for user "+userId);
					counts.put(item, counts.get(item) + count);
				}
		}
		if (this.fillInZerosWithMostPopular)
		{
			res = new ArrayList<>();
			List<Long> cRes = CollectionTools.sortMapAndLimitToList(counts, items.size()); 
			for(Long item : cRes)
				if (counts.get(item) > 0)
					res.add(item);
				else
					break;
		}
		else
			res = CollectionTools.sortMapAndLimitToList(counts, items.size());
		
		return res;
	}
	
	private List<UserCluster> getShortTermClusters(long userId,Integer group)
	{
		//Get Dynamic short-term clusters
		List<UserCluster> clusters = (List<UserCluster>) MemCachePeer.get(MemCacheKeys.getShortTermClustersForUser(client, userId));
		if (clusters != null)
		{
			logger.debug("Got "+clusters.size()+" short term clusters for user "+userId);
		}
		else
		{
			logger.debug("Got 0 short term clusters for user "+userId);
			clusters = new ArrayList<>();
		}
		return clusters;
	}
	
	private List<UserCluster> getClusters(long userId,Integer group)
	{
		List<UserCluster> clusters = null;
		String memcacheKey = null;
		if (userClusters.needsExternalCaching() && !testMode)
		{
			memcacheKey = MemCacheKeys.getClustersForUser(client,userId);
			clusters = (List<UserCluster>) MemCachePeer.get(memcacheKey);
		}
		if (clusters == null)
		{
			clusters = userClusters.getClusters(userId);
			if (userClusters.needsExternalCaching()  && !testMode)
				MemCachePeer.put(memcacheKey, clusters, EXPIRE_USER_CLUSTERS);
		}
		
		
		//prune clusters not in desired group
		if (group != null)
		{
			for(Iterator<UserCluster> i=clusters.iterator();i.hasNext();)
			{
				if (!group.equals(i.next().getGroup()))
					i.remove();
			}
		}
		return clusters;
	}
	
	private Map<Long,Double> getClusterTopCountsForDimension(final int dimension, final int limit, final double decay, final Integer dimension2) throws ClusterCountNoImplementationException {
		if (dimension2 != null)
			return getClusterCounts(MemCacheKeys.getTopClusterCountsForTwoDimensions(client, dimension, dimension2, limit),
					new UpdateRetriever<ClustersCounts>() {
						@Override
						public ClustersCounts retrieve() throws Exception {
							logger.debug("Trying to get top counts for dimension from db : testMode is " + testMode + " for client " + client + " dimension:" + dimension + " dimension2:" + dimension2);
							Map<Long, Double> itemMap = clusterCounts.getTopCountsByTwoDimensions(dimension, dimension2, TestingUtils.getTime(), limit, decay);
							return new ClustersCounts(itemMap, 0);
						}
					}
			);
		else {
			return getClusterCounts(MemCacheKeys.getTopClusterCountsForDimension(client, dimension, limit),
					new UpdateRetriever<ClustersCounts>() {
						@Override
						public ClustersCounts retrieve() throws Exception {
							logger.debug("Trying to get top counts for dimension from db : testMode is " + testMode + " for client " + client + " dimension:" + dimension);
							Map<Long, Double> itemMap = clusterCounts.getTopCountsByDimension(dimension, TestingUtils.getTime(), limit, decay);

							return new ClustersCounts(itemMap, 0);
						}
					}
			);

		}
	}

	private Map<Long, Double> getClusterCounts(String memcacheKey,UpdateRetriever<ClustersCounts> retriever) throws ClusterCountNoImplementationException {
		if(clusterCounts.needsExternalCaching()){
			ClustersCounts itemCounts = (ClustersCounts) MemCachePeer.get(memcacheKey);
			ClustersCounts newItemCounts = null;
			try {
				newItemCounts = DogpileHandler.get().retrieveUpdateIfRequired(memcacheKey, itemCounts, retriever, EXPIRE_COUNTS);
			} catch (Exception e){
				if (e instanceof ClusterCountNoImplementationException){
					throw (ClusterCountNoImplementationException) e;
				}else{
					logger.error("Unknown exception:" , e);
				}
			}
			if(newItemCounts !=null ){
				MemCachePeer.put(memcacheKey, newItemCounts , EXPIRE_COUNTS);
				return newItemCounts.getItemCounts();
			} else {
				if(itemCounts==null){
					logger.debug("Couldn't get cluster counts from store or memcache. Returning null");
					return null;
				} else {
					return itemCounts.getItemCounts();
				}
			}
		} else {
			try {
				return retriever.retrieve().getItemCounts();
			} catch (Exception e) {
				if (e instanceof ClusterCountNoImplementationException){
					throw (ClusterCountNoImplementationException) e;
				}else{
					logger.error("Unknown exception:" , e);
					return null;
				}
			}
		}
	}
	
	private Map<Long,Double> getClusterTopCountsForDimension(String recommenderType,final int clusterId,final int dimension,final long timestamp,final int limit,final double decay) throws ClusterCountNoImplementationException
	{
			switch(recommenderType)
			{
			case "CLUSTER_COUNTS_SIGNIFICANT":
				return getClusterCounts(MemCacheKeys.getTopClusterCountsForDimensionAlg(client, recommenderType, clusterId, dimension, limit),
						new UpdateRetriever<ClustersCounts>() {
							@Override
							public ClustersCounts retrieve() throws Exception{
								logger.debug("Trying to get top counts from db : testMode is " + testMode + " for client " + client + " cluster id:" + clusterId + " dimension:" + dimension + " limit:" + limit);
								Map<Long, Double> itemMap = clusterCounts.getTopSignificantCountsByDimension(clusterId, dimension, timestamp, TestingUtils.getTime(), limit, decay);
								return new ClustersCounts(itemMap, timestamp);
							}
						}
				);
			default:
				return getClusterCounts(MemCacheKeys.getTopClusterCountsForDimension(client, clusterId, dimension, limit),
						new UpdateRetriever<ClustersCounts>() {
							@Override
							public ClustersCounts retrieve() throws Exception {
								logger.debug("Trying to get top counts from db : testMode is " + testMode + " for client " + client + " cluster id:" + clusterId + " dimension:" + dimension + " limit:" + limit);
								Map<Long, Double> itemMap = clusterCounts.getTopCountsByDimension(clusterId, dimension, timestamp, TestingUtils.getTime(), limit, decay);
								return new ClustersCounts(itemMap, timestamp);
							}
						}
				);
			}

	}

	private Map<Long,Double> getClusterTopCounts(final int limit,final double decay) throws ClusterCountNoImplementationException
	{
		return getClusterCounts(MemCacheKeys.getTopClusterCounts(client,limit),new UpdateRetriever<ClustersCounts>() {
			@Override
			public ClustersCounts retrieve() throws Exception {
				logger.debug("Retrieving global cluster top counts from store");
				Map<Long,Double> itemMap = clusterCounts.getTopCounts(TestingUtils.getTime(), limit, decay);
				return new ClustersCounts(itemMap,0);
			}
		});
	}
	
	private Map<Long,Double> getClusterTopCounts(final int clusterId, final long timestamp, final int limit, final double decay) throws ClusterCountNoImplementationException
	{
		return getClusterCounts(MemCacheKeys.getTopClusterCounts(client, clusterId, limit), new UpdateRetriever<ClustersCounts>() {
			@Override
			public ClustersCounts retrieve() throws Exception {
				Map<Long,Double> itemMap = clusterCounts.getTopCounts(clusterId, timestamp, TestingUtils.getTime(), limit, decay);
				return new ClustersCounts(itemMap,timestamp);
			}
		});

	}
	
	private Map<Long,Double> getClusterCounts(final int clusterId, final long timestamp, final List<Long> items)
	{

		try {
			return getClusterCounts(MemCacheKeys.getClusterCountForItems(client, clusterId, items, timestamp), new UpdateRetriever<ClustersCounts>() {
                @Override
                public ClustersCounts retrieve() throws Exception {
                    Map<Long,Double> itemMap = new HashMap<>();
                    for(Long itemId : items)
                    {
                        double count = clusterCounts.getCount(clusterId, itemId, timestamp,TestingUtils.getTime());
                        itemMap.put(itemId, count);
                    }
                    return new ClustersCounts(itemMap,timestamp);
                }
            });
		} catch (ClusterCountNoImplementationException e) {
			// can't happen
			return null;
		}

	}
	
	
	
}
