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

import io.seldon.clustering.recommender.jdo.JdoUserDimCache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * A class to bring together various ways to instantlt map a user to a set of clusters to allow for instant recommendations.
 * @author rummble
 *
 */
public class CreateClustersUtils {

	private static Logger logger = Logger.getLogger(CreateClustersUtils.class.getName());
	
	public static final int MAX_CLUSTERS = 5;
	
	String client;
	TransientUserClusterStore store;
	MemoryUserClusterStore mstore;
	public CreateClustersUtils(String client,TransientUserClusterStore store,MemoryUserClusterStore mstore) {
		super();
		this.client = client;
		this.store = store;
		this.mstore = mstore;
	}

	private void storeClusters(long userId,List<UserCluster> clusters)
	{
		if (clusters != null && clusters.size() > 0)
		{
			if (mstore != null)
				mstore.store(userId, clusters);
			store.addTransientCluster(clusters);
		}
	}

	/**
	 * Create clusters from Facebook categories using a mapping of category to user dimension
	 * @param userId
	 * @param categories
	 * @param map
	 */
	public void createClustersFromFacebookCategories(long userId,List<String> categories,Map<String,Set<Integer>> map)
	{
		FBToUserDimMapper mapper = new FBToUserDimMapper(map);
		List<UserCluster> clusters = mapper.suggestClusters(userId, categories);
		storeClusters(userId,clusters);
	}
	

	/**
	 * Create clusters from a set of tags passed in, e.g. Facebook like names
	 * @param userId - internal user id
	 * @param tags - facebook likes
	 */
	public void createClustersFromTags(long userId,Set<String> tags)
	{
		TagToClusterPeer tc = new TagToClusterPeer(client);
		List<UserCluster> clusters = tc.suggestClusters(userId, tags);
		storeClusters(userId,clusters);
	}
	
	
	/**
	 * Create clusters for user from the tags (e.g. facebook like names "Elton John") and the
	 * ids (e.g. facebook like id)
	 * Will first attempt to use WebSimilaritySimplePeer and if that fails try the tags.
	 * @param userId - internal user id
	 * @param tags - facebook likes
	 * @param ids - facebook like ids
	 */
	public void createClustersFromCategoriesOrTags(long userId,Set<String> tags,List<String> categories)
	{
		Map<String,Set<Integer>> catMap = JdoUserDimCache.get().getEntry(client);
		FBToUserDimMapper mapper = new FBToUserDimMapper(catMap);
		List<UserCluster> clusters = mapper.suggestClusters(userId, categories);
		TagToClusterPeer tc = new TagToClusterPeer(client);
		List<UserCluster> clusters2 = tc.suggestClusters(userId, tags);
		List<UserCluster> res = merge(clusters,clusters2);
		if (res.size() > 0)
		{
			Collections.sort(res, Collections.reverseOrder());
			if (res.size() > MAX_CLUSTERS)
			{
				logger.debug("Limiting cluster size to "+MAX_CLUSTERS+" for user "+userId);
				res = res.subList(0, MAX_CLUSTERS);
			}
			logger.info("Storing "+res.size()+" clusters for "+userId);
			if (logger.isDebugEnabled())
			{
				for(UserCluster c : res)
					logger.info("will store "+c.toString());
			}
			storeClusters(userId,res);
		}
		else
			logger.warn("No clusters created for user "+userId);
	}
	
	public List<UserCluster> merge(List<UserCluster> c1,List<UserCluster> c2)
	{
		List<UserCluster> res = new ArrayList<>(c1);
		for(UserCluster c : c2)
		{
			int index;
			if ((index = res.indexOf(c)) > -1)
			{
				UserCluster cb = res.get(index);
				cb.merge(c);
			}
			else
				res.add(c);
		}
		return res;
	}
	
}
