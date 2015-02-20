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

package io.seldon.trust.impl;

import java.util.List;

import io.seldon.api.resource.DimensionBean;
import io.seldon.api.resource.RecommendedUserBean;

public interface RummbleLabsAnalysis {
	
	public RecommendationResult getRecommendations(long userId,String clientUserId,Integer type,int dimension,int numRecommendations,CFAlgorithm options,String lastRecListUUID,Long currentItemId, String referrer);
	public List<SearchResult> searchContent(String query,Long userId,DimensionBean d,int numResults,CFAlgorithm options);
	public List<SearchResult> findSimilar(long itemId,int itemType,int numResults,CFAlgorithm options);
	public RecommendationNetwork getNetwork(long userId, int dimension, CFAlgorithm cfAlgorithm);
	public SortResult sort(Long userId,List<Long> items, CFAlgorithm options, List<Long> recentActions);
	public List<RecommendedUserBean> sharingRecommendation(String userFbId,long userId,Long itemId,String linkype,List<String> tags,int limit,CFAlgorithm options);
	public List<SearchResult> getSimilarUsers(long userId,int limit, int filterType, CFAlgorithm options);
}
