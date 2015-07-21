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
import io.seldon.tags.UserTagAffinityManager;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class TagAffinityFilterModelManager extends ModelManager<TagAffinityFilterModelManager.TagAffinityFilterModel> {

	private static Logger logger = Logger.getLogger(UserTagAffinityManager.class.getName());
	private final ExternalResourceStreamer featuresFileHandler;
    public static final String FILTER_NEW_LOC_PATTERN = "tagcluster";


   
	
	public static class TagAffinityFilterModel {
		final Map<String,Integer> groupNameToIdMap;
		final Map<Integer,String> clusterIdToNameMap;
		final Map<Integer,Set<Integer>> groupToClustersMap;
		final Map<Integer,Set<String>> groupToTagsMap;
		final Map<Long,Set<Integer>> userToClustersMap;
		
		public TagAffinityFilterModel(Map<String, Integer> groupNameToIdMap,
				Map<Integer, String> clusterIdToNameMap,
				Map<Integer, Set<Integer>> groupToClustersMap,
				Map<Integer, Set<String>> groupToTagsMap,
				Map<Long, Set<Integer>> userToClustersMap) {
			super();
			this.groupNameToIdMap = groupNameToIdMap;
			this.clusterIdToNameMap = clusterIdToNameMap;
			this.groupToClustersMap = groupToClustersMap;
			this.groupToTagsMap = groupToTagsMap;
			this.userToClustersMap = userToClustersMap;
		}

		
		
	}


	@Override
	protected TagAffinityFilterModel loadModel(String location, String client) {
		// TODO Auto-generated method stub
		return null;
	}

	
}
