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

import java.util.Properties;

import io.seldon.cc.UserClusterManager;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.MemoryClusterCountFactory;
import io.seldon.clustering.recommender.UserClusterStore;
import io.seldon.db.jdo.ClientPersistable;
import org.apache.log4j.Logger;

import io.seldon.clustering.recommender.ClusterCountStore;
import io.seldon.clustering.recommender.MemcacheClusterCountFactory;

public class JdoCountRecommenderUtils extends ClientPersistable {

	private static Logger logger = Logger.getLogger(JdoCountRecommenderUtils.class.getName());
	
	public static boolean memoryBasedOnly = true;
	
	public static void initialise(Properties props)
	{
		String memOnly = props.getProperty("io.seldon.clusters.memoryonly");
		if (memOnly != null)
		{
			memoryBasedOnly = Boolean.parseBoolean(memOnly);
		}
		logger.info("Memory cluster only set to:"+memoryBasedOnly);
	}
	
	public JdoCountRecommenderUtils(String client) {
		super(client);
	}

	public CountRecommender getCountRecommender(String client)
	{
		// Get cluster counter
		ClusterCountStore counter = null;
		MemcacheClusterCountFactory mFac = MemcacheClusterCountFactory.get();
		if (mFac != null)
			counter = mFac.getStore(client);
		if (counter == null)
		{
			MemoryClusterCountFactory mClFac = MemoryClusterCountFactory.get();
			if (mClFac != null)
				counter = mClFac.getStore(client);
			if (memoryBasedOnly && counter == null)
				return null;
			else if (counter == null)
				counter = new JdoClusterCountStore(client); // Database backed count store
		}
		// get user clusters
		UserClusterStore userClusters = null;
		userClusters = UserClusterManager.get().getStore(client); // Hack until we always use this class
		if (userClusters == null)
		{
			JdoMemoryUserClusterFactory memUserFac = JdoMemoryUserClusterFactory.get();
			if (memUserFac != null)
				userClusters = memUserFac.get(client);
			if (memoryBasedOnly && userClusters == null)
				return null;
			else if (userClusters == null) 
				userClusters = new JdoUserClusterStore(getPM());
		}
		
		return new CountRecommender(client,userClusters,counter);
	}

	
}
