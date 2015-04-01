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

package io.seldon.recommendation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.trust.impl.RecommendationNetwork;
import io.seldon.util.CollectionTools;

public class RecommendationNetworkImpl implements Serializable, RecommendationNetwork {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2146123935757353218L;
	long user;
	int type;
	Map<Long,Double> trust;
	Map<Long,Double> userAvg; // Avg rating for user for shared content against another user

	public RecommendationNetworkImpl(long user,int type)
	{
		this.user = user;
		this.type = type;
	}

	public int getType() {
		return type;
	}

	public void setTrust(Map<Long, Double> trust) {
		this.trust = trust;
	}

	public long getUser() {
		return user;
	}

	public Map<Long, Double> getTrust() {
		return trust;
	}
	
	public List<Long> getTrustNeighbourhoodUsers(int k)
	{
		return CollectionTools.sortMapAndLimitToList(trust, k);
	}
	
	public Map<Long,Double> getTrustNeighbourhood(int k)
	{
		return CollectionTools.sortMapAndLimit(trust, k);
	}

	@Override
	public long getId() {
		return getUser();
	}

	@Override
	public Double getSimilarity(long id) {
		return getTrust().get(id);
	}

	@Override
	public List<Long> getSimilarityNeighbourhood(int k) {
		return getTrustNeighbourhoodUsers(k);
	}

	@Override
	public Set<Long> getSimilarityNetwork() {
		return trust.keySet();
	}

	@Override
	public Map<Long, Double> getSimilarityNeighbourhoodMap(int k) {
		return getTrustNeighbourhood(k);
	}
}
