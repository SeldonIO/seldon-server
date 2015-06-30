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

import io.seldon.semvec.DocumentIdTransform;
import io.seldon.semvec.SemVectorResult;
import io.seldon.semvec.StringTransform;
import io.seldon.sv.SemanticVectorsManager;
import io.seldon.sv.SemanticVectorsStore;
import io.seldon.util.CollectionTools;
import org.apache.log4j.Logger;

/**
 * Map tags (could be Facebook likes) to clusters using a semantic vectors store that has all the article texts for
 * each user dimension. We find for all the tags which user_dim documents match the best and use these as the clusters.
 * @author rummble
 *
 */
public class TagToClusterPeer {

	private static Logger logger = Logger.getLogger(TagToClusterPeer.class.getName());
	
	private static final int NUM_CLUSTERS_PER_SEARCH = 10;
	private static final int NUM_CLUSTERS_RETURNED = 10;
	// if the results are bad they will effect recommendations for other users as this user's views will begin to be
	// seen in the clusters they are assigned. Thus is safer to give a low maximum weight.
	private static final double MAX_WEIGHT = 0.5; 
	private String client;
	
	public TagToClusterPeer(String client)
	{
		this.client = client;
	}
	
	private String getSearchTerm(String tag)
	{
		if (tag != null)
		{
			String parts[] = tag.toLowerCase().trim().split("\\s+");
			if (parts.length > 3)
				return null;
			else
			{
				StringBuffer b = new StringBuffer();
				for(int i=0;i<parts.length;i++)
					if (i>0)
						b.append("_").append(parts[i]);
					else
						b.append(parts[i]);
				return b.toString();
			}
		}
		else
			return null;
	}
	
	public List<UserCluster> suggestClusters(long userId,Set<String> tags)
	{
		List<UserCluster> clusters = new ArrayList<>();
		//TODO code needs to be Springified so static method below can be removed
		SemanticVectorsStore sem = null;
//		sem = SemanticVectorsManager.getManager().getStore(client,SemanticVectorsManager.SV_CLUSTER_NEW_LOC_PATTERN,ctxt);
		if (sem != null)
		{
			Map<Long,Double> scores = new HashMap<>();
			for(String tag : tags)
			{
				logger.info("For user "+userId+" checking ["+tag+"]");
				String searchTerm = getSearchTerm(tag);
				if (searchTerm != null)
				{
					logger.info("Will search with term "+searchTerm);
					ArrayList<SemVectorResult<Long>> results = new ArrayList<>();
					sem.searchDocsUsingTermQuery(searchTerm, results, new DocumentIdTransform(),new StringTransform(),NUM_CLUSTERS_PER_SEARCH);
					for(SemVectorResult<Long> r : results)
					{
						logger.info("Searching ["+searchTerm+"] dim_id:"+r.getResult()+"score:"+r.getScore());
						if (r.getScore() > 0.5)
						{
							logger.info("Adding dim_id "+r.getResult()+" with score "+r.getScore());
							if (scores.containsKey(r.getResult()))
								scores.put(r.getResult(), scores.get(r.getResult()) + r.getScore());
							else
								scores.put(r.getResult(), r.getScore());
						}
					}
				}
			}
			if (scores.size() > 0)
			{
				List<Long> best = CollectionTools.sortMapAndLimitToList(scores, NUM_CLUSTERS_RETURNED);
				double bestScore = scores.get(best.get(0));
				for(Long id : best)
				{
					logger.info("Choosing dim_id:"+id+" with score "+scores.get(id));
					double weight = MAX_WEIGHT * (scores.get(id)/bestScore);
					UserCluster cluster = new UserCluster(userId,id.intValue(),weight,0L,0);
					clusters.add(cluster);
				}
			}
		}
		return clusters;
	}
	
}
