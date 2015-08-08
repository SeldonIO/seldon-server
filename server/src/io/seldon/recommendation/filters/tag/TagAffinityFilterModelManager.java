/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.recommendation.filters.tag;

import io.seldon.recommendation.model.ModelManager;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class TagAffinityFilterModelManager extends ModelManager<TagAffinityFilterModelManager.TagAffinityFilterModel> {

	private static Logger logger = Logger.getLogger(TagAffinityFilterModelManager.class.getName());
	private final ExternalResourceStreamer featuresFileHandler;
    public static final String FILTER_NEW_LOC_PATTERN = "tagcluster";

    @Autowired
    public TagAffinityFilterModelManager(ExternalResourceStreamer featuresFileHandler,NewResourceNotifier notifier) {
		super(notifier, Collections.singleton(FILTER_NEW_LOC_PATTERN));
		this.featuresFileHandler = featuresFileHandler;
	}

    private Map<Long,Set<Integer>> loadUserTagClusters(BufferedReader reader) throws IOException
    {
    	String line;
    	ObjectMapper mapper = new ObjectMapper();
    	Map<Long,Set<Integer>> userToClustersMap = new ConcurrentHashMap<Long,Set<Integer>>();
    	while ((line = reader.readLine()) != null) {
    		UserTagCluster userCluster = mapper.readValue(line.getBytes(),UserTagCluster.class);
    		Set<Integer> clusters = userToClustersMap.get(userCluster.user);
    		if (clusters == null)
    		{
    			clusters = new HashSet<Integer>();
    			userToClustersMap.put(userCluster.user, clusters);
    		}
    		clusters.add(userCluster.cluster);
    	}
    	return userToClustersMap;
    }
    
    private TagAffinityFilterModel loadClusters(String client,Map<Long,Set<Integer>> userToClustersMap,BufferedReader reader) throws IOException, NumberFormatException
    {
    	String line;
    	ObjectMapper mapper = new ObjectMapper();
    	Map<Integer,Set<String>> clusterToTagsMap = new ConcurrentHashMap<Integer,Set<String>>();
    	Map<Integer,Set<Integer>> groupToClustersMap = new ConcurrentHashMap<Integer,Set<Integer>>();
    	int numTags = 0;
    	while ((line = reader.readLine()) != null) {
    		String[] parts = line.split(",");
    		String tag = parts[0];
    		int group = Integer.parseInt(parts[1]);
    		int cluster = Integer.parseInt(parts[2]);
    		
    		Set<String> tags = clusterToTagsMap.get(cluster);
    		if (tags == null)
    		{
    			tags = new HashSet<String>();
    			clusterToTagsMap.put(cluster, tags);
    		}
    		tags.add(tag);
    		numTags++;
    		
    		Set<Integer> clusters = groupToClustersMap.get(group);
    		if (clusters == null)
    		{
    			clusters = new HashSet<Integer>();
    			groupToClustersMap.put(group, clusters);
    		}
    		clusters.add(cluster);
    	}
    	logger.info("Loaded "+numTags+" tags for "+client);
		return new TagAffinityFilterModel(groupToClustersMap, clusterToTagsMap, userToClustersMap);
    }

	@Override
	protected TagAffinityFilterModel loadModel(String location, String client) {
		 logger.info("Reloading user tag clusters for client: " + client);
		 

		 try
		 {
			 BufferedReader reader = new BufferedReader(new InputStreamReader(featuresFileHandler.getResourceStream(location + "/part-00000")));
			 Map<Long,Set<Integer>> userClusters = loadUserTagClusters(reader);
			 reader.close();
			 
			 reader = new BufferedReader(new InputStreamReader(featuresFileHandler.getResourceStream(location + "/tags.csv")));
			 TagAffinityFilterModel model = loadClusters(client,userClusters, reader);
			 
			 logger.info("Loaded tag clusters for "+client+" userClusters:"+model.userToClustersMap.size()+" clusters:"+model.clusterToTagsMap.size()+" groups:"+model.groupToClustersMap.size());
			 
			 return model;
			 
		 } catch (IOException e) 
		 {
			 logger.error("Couldn't reloadFeatures for client " + client, e);
			 return null;
	     }
		 catch (Exception e)
		 {
			 logger.error("Couldn't reloadFeatures for client " + client, e);
			 return null;
		 }

	}


	
	public static class TagAffinityFilterModel {
		final Map<Integer,Set<Integer>> groupToClustersMap;
		final Map<Integer,Set<String>> clusterToTagsMap;
		final Map<Long,Set<Integer>> userToClustersMap;
		public TagAffinityFilterModel(
				Map<Integer, Set<Integer>> groupToClustersMap,
				Map<Integer, Set<String>> clusterToTagsMap,
				Map<Long, Set<Integer>> userToClustersMap) {
			super();
			this.groupToClustersMap = groupToClustersMap;
			this.clusterToTagsMap = clusterToTagsMap;
			this.userToClustersMap = userToClustersMap;
		}
		
		
	}






}
