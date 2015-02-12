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

package io.seldon.similarity.dbpedia;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.clustering.recommender.UserCluster;
import io.seldon.trust.impl.SharingRecommendation;

public interface WebSimilaritySimpleStore {

	public List<SharingRecommendation> getSharingRecommendations(List<Long> users,List<Long> items);
	public List<SharingRecommendation> getSharingRecommendationsForFriends(long userid,String linkType,List<Long> items);
	public List<SharingRecommendation> getSharingRecommendationsForFriends(long userid,long itemId);
	public List<SharingRecommendation> getSharingRecommendationsForFriends(long userid,List<String> tags);
	public List<UserCluster> getUserDimClusters(long userid,Set<String> clientItemIds);
	public boolean hasBeenSearched(long itemId);
	public List<SharingRecommendation> getSharingRecommendationsForFriendsFull(long userId, long itemId);
	public Map<Long,Double> getSocialPredictionRecommendations(long userId,int limit);
	public Map<Long,Double> getSimilarUsers(long userId,int type, int interactionFilterType);
	
}
