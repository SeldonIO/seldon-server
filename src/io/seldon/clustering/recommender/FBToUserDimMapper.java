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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.util.CollectionTools;
import org.apache.log4j.Logger;

/**
 * Use a map of Facebook categories to user dimensions to simply find the best user dimension clusters to suggest 
 * for a user
 * @author rummble
 *
 */
public class FBToUserDimMapper {

	private static Logger logger = Logger.getLogger(FBToUserDimMapper.class.getName());
	
	Map<String,Set<Integer>> categoryToDim;
	private static final double MAX_WEIGHT = 0.75; 
	
	public FBToUserDimMapper(Map<String,Set<Integer>> map)
	{
		this.categoryToDim = map;
	}
	
	public List<UserCluster> suggestClusters(long userId,List<String> categories)
	{
		List<UserCluster> clusters = new ArrayList<UserCluster>();
		Map<Integer,Integer> scores = new HashMap<Integer,Integer>();
		for(String category : categories)
		{
			logger.info("Trying to map " + category+" for user "+userId);
			Set<Integer> dims = categoryToDim.get(category);
			if (dims != null)
			{
				for(Integer dim : dims)
				{
					if (dim>0)
					{
						if (scores.containsKey(dim))
							scores.put(dim, scores.get(dim)+1);
						else
							scores.put(dim, 1);
					}
				}
			}
		}
		if (scores.size() > 0)
		{
			List<Integer> top = CollectionTools.sortMapAndLimitToList(scores, 10);
			double bestScore = scores.get(top.get(0));
			for(Integer id : top)
			{
				double weight = MAX_WEIGHT * (scores.get(id)/bestScore);
				logger.info("user "+userId+" dim:"+id+" score:"+scores.get(id)+" weight:"+weight);
				UserCluster cluster = new UserCluster(userId,id.intValue(),weight,0L,0);
				clusters.add(cluster);
			}
		}
		return clusters;
	}
	
}
